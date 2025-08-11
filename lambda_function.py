import os
from tracemalloc import start
from dotenv import load_dotenv
import json
from datetime import date, datetime, timedelta
import pandas as pd

env_path = os.path.join(os.path.dirname(__file__), "config", ".env")
load_dotenv(dotenv_path=env_path)

print("ENV VARS:", dict(os.environ))

from lambda_utils import (
    create_database_tables,
    lambda_response,
    get_season_date_ranges,
    RDSConnection,
    logger
)

# EXPLICIT FETCHER IMPORTS
from data_fetchers.boxscore_fetcher import BoxscoreFetcher
from data_fetchers.statcast_fetcher import StatcastFetcher

# Team abbreviations from Baseball Reference
TEAMS = [
    "ARI","ATL","BAL","BOS","CHC","CIN","CLE","COL","DET","HOU",
    "KC", "LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI",
    "PIT","SD","SEA","SF","STL","TB","TEX","TOR","WAS","CHW"
]

def process_single_date(target_date: date, include_probables: bool = True) -> dict:
    """
    Process a single date - fetch boxscores, statcast data, and probables (optional).
    Used by both daily_update and historical_raw

    Returns:
        dict: Results with success status and row counts
    """
    tgt_iso = target_date.strftime("%Y-%m-%d")
    logger.info(f"Processing {tgt_iso}")

    # Check if date is within season bounds
    season_bounds = get_season_date_ranges().get(target_date.year)
    if season_bounds:
        start_s, end_s = season_bounds
        if not (start_s <= target_date <= end_s):
            logger.warning(f"Date {tgt_iso} is outside of season bounds {start_s} - {end_s}")
            return {
                "boxscores":{"success": True, "rows": 0, "skipped": True},
                "statcast":{
                    "pitchers":{"success":True, "rows": 0, "skipped": True},
                    "batters": {"success":True, "rows": 0, "skipped": True}
                },
                "probables": {"success": True, "rows": 0, "skipped": True},
                "date": tgt_iso
            }
        
    results = {
        "boxscores": {"success": True, "rows": 0},
        "statcast": {
            "pitchers": {"success": True, "rows": 0},
            "batters": {"success": True, "rows": 0}
        },
        "probables":{"success":True, "rows":0},
        "date": tgt_iso
    }

    try:
        # 1. Boxscores for this date
        logger.info(f"Fetching boxscores for {tgt_iso}")
        box_res = BoxscoreFetcher.fetch_and_store_date(tgt_iso)
        results["boxscores"] = box_res
        logger.info(f"Boxscores result: {box_res}")

        # 2. Probables for the next day if requested
        if include_probables:
            try:
                tomorrow = (target_date + timedelta(days=1)).strftime("%Y-%m-%d")
                logger.info(f"Fetching probable pitchers for {tomorrow}")
                prob_rows = BoxscoreFetcher._fetch_probable_pitchers_for_date(tomorrow)
                if prob_rows:
                    df_prob = pd.DataFrame(prob_rows)
                    prob_written = BoxscoreFetcher._upsert_to_rds(
                        df_prob,
                        table="probable_pitchers",
                        conflic_column="game_pk"
                    )
                    results["probables"] = {"success": True, "rows": prob_written}
                    logger.info(f"Probables result: {prob_written} rows")
                else:
                    results["probables"] = {"success": True, "rows": 0}
            except Exception as e:
                logger.error(f"Error fetching probable pitchers for {tomorrow}: {e}")
                results["probables"] = {"success":False, "rows":0, "error": str(e)}

        # 3. Statcast data 
        logger.info(f"Fetching statcast data for {tgt_iso}")
        sf = StatcastFetcher()

        # Statcast Pitchers
        pit_res = sf.fetch_and_store_pitchers(tgt_iso, tgt_iso)
        results["statcast"] ["pitchers"] = pit_res
        logger.info(f"Statcast Pitchers result: {pit_res}")

        # Statcast Batters
        bat_res = sf.fetch_and_store_batters(tgt_iso, tgt_iso)
        results["statcast"]["batters"] = bat_res
        logger.info(f"Statcast Batters result: {bat_res}")

    except Exception as e:
        logger.error(f"Error processing {tgt_iso}: {e}", exc_info=True)
        # Mark all as failed
        results["boxscores"]["success"] = False
        results["statcast"]["pitchers"]["success"] = False
        results["statcast"]["batters"]["success"] = False
        results["probables"]["success"] = False
        results["error"] = str(e)

    return results

def lambda_handler(event, context):
    """
    Modes:
        - historical_raw -> full backfill of boxscores, statcast & team data (run one time)
        - historical_agg -> aggregate pitcher stats into different tables (one time)
        - daily_update -> automated single day fetch + aggregations (recurring)
        - health_check -> connection check between DB & MLB API
    """

    # Make /tmp writable for pybaseball cache
    os.environ["HOME"] = "/tmp"

    # Parse inputs
    mode = event.get("mode", "daily_update")
    sd_str = event.get("start_date")
    ed_str = event.get("end_date")
    tgt_str = event.get("target_date")

    logger.info(f"Invoked mode: {mode}, start={sd_str}, end={ed_str}, target={tgt_str}")
    create_database_tables()

    try:
        # --- HISTORICAL RAW ---
        if mode == "historical_raw":
            if not (sd_str and ed_str):
                return lambda_response(400, "historical_raw requires start_date and end_date")
            
            start_date = datetime.strptime(sd_str, "%Y-%m-%d").date()
            end_date = datetime.strptime(ed_str, "%Y-%m-%d").date()

            logger.info(f"Starting historical backfill from {start_date} to {end_date}")

            # Process each date invidually
            all_results = {
                "boxscores": {"success": True, "total_rows": 0, "failed_dates": []},
                "statcast": {
                    "pitchers": {"success": True, "total_rows": 0, "failed_dates": []},
                    "batters": {"success": True, "total_rows": 0, "failed_dates": []},
                },
                "probables": {"success": True, "total_rows": 0, "failed_dates": []},
                "dates_processed": 0,
                "dates_skipped": 0,
                "dates_failed": [],
                "daily_results": []
            }

            for process_date in dates_to_process:
                # Process single date (no probables for historical_raw)
                day_result = process_single_date(process_date, include_probables=False)
                all_results["daily_results"].append(day_result)

                # Check if date was skipped
                if day_result.get("boxscores", {}).get("skipped", False):
                    all_results["dates_skipped"] += 1
                    continue

                all_results["dates_processed"] += 1

                # Aggregate results
                for section in ["boxscores"]:
                    result = day_result.get(section, {})
                    if result.get("success", False):
                        all_results[section]["total_rows"] += result.get("rows", 0)
                    else:
                        all_results[section]["success"] = False
                        all_results[section]["failed_dates"].append(process_date.strftime("%Y-%m-%d"))

                # Handle nested statcast results
                for subsection in ["pitchers", "batters"]:
                    result = day_result.get("statcast", {}).get(subsection, {})
                    if result.get("success", False):
                        all_results["statcast"][subsection]["total_rows"] += result.get("rows", 0)
                    else:
                        all_results["statcast"][subsection]["success"] = False
                        all_results["statcast"][subsection]["failed_dates"].append(process_date.strftime("%Y-%m-%d"))

                # Track overall failures
                if "error" in day_result:
                    all_results["dates_failed"].append(process_date.strftime("%Y-%m-%d"))

            # Determine overall success
            overall_success = (
                all_results["boxscores"]["success"] and
                all_results["statcast"]["pitchers"]["success"] and
                all_results["statcast"]["batters"]["success"]
            )

            code = 200 if overall_success else 206

            if overall_success:
                msg = f"Historical backfill complete: {all_results['dates_processed']} dates processed"
            else:
                failed_sections = []
                if not all_results["boxscores"]["success"]:
                    failed_sections.append("boxscores")
                if not all_results["statcast"]["pitchers"]["success"]:
                    failed_sections.append("pitchers")
                if not all_results["statcast"]["batters"]["success"]:
                    failed_sections.append("batters")

                msg = f"Partial backfill: {all_results['dates_processed']} dates processed, failed sections: {failed_sections}"

            return lambda_response(code, msg, all_results)

        # ─────────────── DAILY UPDATE ───────────────
        elif mode == "daily_update":
            # Determine date(s) to process
            dates_to_process = []
            
            if tgt_str:
                # Single target date
                dates_to_process = [datetime.strptime(tgt_str, "%Y-%m-%d").date()]
                
            elif sd_str and ed_str:
                # Date range (or single date passed as start/end)
                start_date = datetime.strptime(sd_str, "%Y-%m-%d").date()
                end_date = datetime.strptime(ed_str, "%Y-%m-%d").date()
                
                # Generate all dates in range
                current_date = start_date
                while current_date <= end_date:
                    dates_to_process.append(current_date)
                    current_date += timedelta(days=1)
                    
                logger.info(f"Processing date range: {start_date} to {end_date} ({len(dates_to_process)} days)")
                
            else:
                # Default to yesterday
                dates_to_process = [date.today() - timedelta(days=1)]
                logger.info("No dates specified, defaulting to yesterday")

            # Process each date using the shared function
            all_results = {
                "boxscores": {"success": True, "rows": 0},
                "statcast": {
                    "pitchers": {"success": True, "rows": 0},
                    "batters": {"success": True, "rows": 0},
                },
                "probables": {"success": True, "rows": 0},
                "dates_processed": 0,
                "failed_dates": []
            }
            
            for process_date in dates_to_process:
                day_result = process_single_date(process_date, include_probables=True)
                
                # Skip if outside season
                if day_result.get("boxscores", {}).get("skipped", False):
                    continue
                    
                all_results["dates_processed"] += 1
                
                # Aggregate results
                all_results["boxscores"]["rows"] += day_result.get("boxscores", {}).get("rows", 0)
                all_results["statcast"]["pitchers"]["rows"] += day_result.get("statcast", {}).get("pitchers", {}).get("rows", 0)
                all_results["statcast"]["batters"]["rows"] += day_result.get("statcast", {}).get("batters", {}).get("rows", 0)
                all_results["probables"]["rows"] += day_result.get("probables", {}).get("rows", 0)
                
                # Check for failures
                if not day_result.get("boxscores", {}).get("success", True):
                    all_results["boxscores"]["success"] = False
                    all_results["failed_dates"].append(f"{process_date}-boxscores")
                    
                if not day_result.get("statcast", {}).get("pitchers", {}).get("success", True):
                    all_results["statcast"]["pitchers"]["success"] = False
                    all_results["failed_dates"].append(f"{process_date}-pitchers")
                    
                if not day_result.get("statcast", {}).get("batters", {}).get("success", True):
                    all_results["statcast"]["batters"]["success"] = False
                    all_results["failed_dates"].append(f"{process_date}-batters")
                    
                if not day_result.get("probables", {}).get("success", True):
                    all_results["probables"]["success"] = False
                    all_results["failed_dates"].append(f"{process_date}-probables")

            # Compute overall status
            all_ok = (
                all_results["boxscores"]["success"] and
                all_results["statcast"]["pitchers"]["success"] and
                all_results["statcast"]["batters"]["success"] and
                all_results["probables"]["success"]
            )
            
            code = 200 if all_ok else 206

            if all_results["failed_dates"]:
                msg = f"Processed {len(dates_to_process)} dates with failures: {all_results['failed_dates']}"
            else:
                if len(dates_to_process) == 1:
                    msg = f"Daily update for {dates_to_process[0]} complete"
                else:
                    msg = f"Daily update for {len(dates_to_process)} dates complete"
            
            return lambda_response(code, msg, all_results)
            
        # ─────────────── UNKNOWN MODE ───────────────
        else:
            return lambda_response(400, f"Unknown mode: {mode}")

    except Exception as e:
        logger.error("Unhandled error in lambda_handler", exc_info=True)
        return lambda_response(500, f"Internal error: {str(e)}")