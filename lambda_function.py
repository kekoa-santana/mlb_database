import os
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

# EXPLICIT AGGREGATOR IMPORTS
# from aggregators.pitcher_aggregator import PitcherAggregator

# Team abbreviations from Baseball Reference
TEAMS = [
    "ARI","ATL","BAL","BOS","CHC","CIN","CLE","COL","DET","HOU",
    "KC", "LAA","LAD","MIA","MIL","MIN","NYM","NYY","OAK","PHI",
    "PIT","SD","SEA","SF","STL","TB","TEX","TOR","WAS","CHW"
]

def lambda_handler(event, context):
    """
    Modes:
      - historical_raw   → full backfill of boxscores, statcast & team data
      - historical_agg   → backfill of pitcher aggregations
      - daily_update     → single-day fetch + aggregations
      - health_check     → sanity‐check DB & MLB API
    """
    # make /tmp writable for pybaseball cache
    os.environ["HOME"] = "/tmp"

    # parse inputs
    mode    = event.get("mode", "daily_update")
    sd_str  = event.get("start_date")
    ed_str  = event.get("end_date")
    tgt_str = event.get("target_date")

    logger.info(f"Invoked mode={mode}, start={sd_str}, end={ed_str}, target={tgt_str}")
    create_database_tables()

    start_date = datetime.strptime(sd_str, "%Y-%m-%d").date()
    end_date = datetime.strptime(ed_str, "%Y-%m-%d").date()

    season_start = start_date.year
    season_end = end_date.year
    
    seasons = list(range(season_start, season_end + 1))

    try:
        # ─────────────── HISTORICAL RAW ───────────────
        if mode == "historical_raw":
            if not (sd_str and ed_str):
                return lambda_response(400, "historical_raw requires start_date and end_date")

            # 1) Boxscores
            logger.info(f"≡ Starting boxscore backfill from {sd_str} to {ed_str}")
            box_res = BoxscoreFetcher.fetch_and_store_range(sd_str, ed_str)
            logger.info(f"→ Boxscore backfill result: {box_res}")

            # 2) Statcast (pitchers & batters)
            sf      = StatcastFetcher()
            pit_res = sf.fetch_and_store_pitchers(sd_str, ed_str)
            bat_res = sf.fetch_and_store_batters(sd_str, ed_str)
        
            results = {
                "boxscores": BoxscoreFetcher.fetch_and_store_range(sd_str, ed_str),
                "statcast": {
                    "pitchers": StatcastFetcher().fetch_and_store_pitchers(sd_str, ed_str),
                    "batters":  StatcastFetcher().fetch_and_store_batters( sd_str, ed_str),
                },
            }

            # update your success/failure logic to handle nested dicts:
            # Flatten every r.get("success") check
            all_ok = True
            for name, section in results.items():
                 #  If this dict is a *group* (no direct 'success' key), dive one level deeper
                if isinstance(section, dict) and "success" not in section:
                    for subkey, r in section.items():
                        all_ok &= r.get("success", False)
                # ────────────────────────────────────────────────
                #  Otherwise this is a single-fetcher result‐dict
                else:
                    all_ok &= section.get("success", False)
            code = 200 if all_ok else 206

            # Build a list of failures by key
            failures = []
            for name, section in results.items():
                if isinstance(section, dict) and "success" not in section:
                    for subkey, r in section.items():
                        if not r.get("success", True):
                            failures.append(f"{name}:{subkey}")
                else:
                    if not section.get("success", True):
                        failures.append(name)

            msg = (
                "Historical backfill complete"
                if all_ok
                else f"Partial backfill; failures in {failures}"
            )
            return lambda_response(code, msg, results)

        # ─────────────── HISTORICAL AGGREGATIONS ───────────────
        # elif mode == "historical_agg":
        #     if not (sd_str and ed_str):
        #         return lambda_response(400, "historical_agg requires start_date and end_date")

        #     agg_res = PitcherAggregator.aggregate_period(sd_str, ed_str)
        #     success = agg_res.get("success", False)
        #     code    = 200 if success else 206
        #     msg     = (
        #         "Historical aggregations complete"
        #         if success
        #         else "Historical aggregations encountered errors"
        #     )
        #     return lambda_response(code, msg, {"pitcher_aggs": agg_res})

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

            # Process each date
            all_results = {
                "boxscores": {"success": True, "rows": 0},
                "statcast": {
                    "pitchers": {"success": True, "rows": 0},
                    "batters": {"success": True, "rows": 0},
                },
                "probables": {"success": True, "rows": 0},
                "dates_processed": len(dates_to_process)
            }
            
            failed_dates = []
            
            for process_date in dates_to_process:
                tgt_iso = process_date.strftime("%Y-%m-%d")
                logger.info(f"Processing date: {tgt_iso}")
                
                # Skip dates outside season
                season_bounds = get_season_date_ranges().get(process_date.year)
                if season_bounds:
                    start_s, end_s = season_bounds
                    if not (start_s <= process_date <= end_s):
                        logger.info(f"Date {tgt_iso} outside season—skipping")
                        continue

                try:
                    # 1) Boxscores for this date
                    box_res = BoxscoreFetcher.fetch_and_store_date(tgt_iso)
                    all_results["boxscores"]["rows"] += box_res.get("rows", 0)
                    if not box_res.get("success", False):
                        all_results["boxscores"]["success"] = False
                        failed_dates.append(f"{tgt_iso}-boxscores")

                    # 2) Probables for next day
                    tomorrow = (process_date + timedelta(days=1)).strftime("%Y-%m-%d")
                    prob_rows = BoxscoreFetcher._fetch_probable_pitchers_for_date(tomorrow)
                    if prob_rows:
                        df_prob = pd.DataFrame(prob_rows)
                        prob_written = BoxscoreFetcher._upsert_to_rds(
                            df_prob,
                            table="probable_pitchers", 
                            conflict_column="game_pk"
                        )
                        all_results["probables"]["rows"] += prob_written
                    
                    # 3) Statcast
                    sf = StatcastFetcher()
                    pit_res = sf.fetch_and_store_pitchers(tgt_iso, tgt_iso)
                    bat_res = sf.fetch_and_store_batters(tgt_iso, tgt_iso)
                    
                    all_results["statcast"]["pitchers"]["rows"] += pit_res.get("rows", 0)
                    all_results["statcast"]["batters"]["rows"] += bat_res.get("rows", 0)
                    
                    if not pit_res.get("success", False):
                        all_results["statcast"]["pitchers"]["success"] = False
                        failed_dates.append(f"{tgt_iso}-pitchers")
                        
                    if not bat_res.get("success", False):
                        all_results["statcast"]["batters"]["success"] = False
                        failed_dates.append(f"{tgt_iso}-batters")

                except Exception as e:
                    logger.error(f"Error processing {tgt_iso}: {e}")
                    failed_dates.append(f"{tgt_iso}-error")
                    continue

            # Compute overall status
            all_ok = (
                all_results["boxscores"]["success"] and
                all_results["statcast"]["pitchers"]["success"] and
                all_results["statcast"]["batters"]["success"] and
                all_results["probables"]["success"]
            )
            
            code = 200 if all_ok else 206

            if failed_dates:
                msg = f"Processed {len(dates_to_process)} dates with failures: {failed_dates}"
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
        logger.error("Unhandled error in lambda_handler", exc_info=e)
        return lambda_response(500, f"Internal error: {str(e)}")