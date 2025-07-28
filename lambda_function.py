import os
from dotenv import load_dotenv
import json
from datetime import date, datetime, timedelta

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
from data_fetchers.boxscore_fetcher import (
    fetch_boxscores_for_range,
    fetch_boxscores_for_date
)
from data_fetchers.statcast_fetcher import StatcastFetcher
from data_fetchers.team_fetcher import TeamFetcher

# EXPLICIT AGGREGATOR IMPORTS
from aggregators.pitcher_aggregator import PitcherAggregator


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
            box_res = fetch_boxscores_for_range(sd_str, ed_str)

            # 2) Statcast (pitchers & batters)
            sf      = StatcastFetcher()
            pit_res = sf.fetch_and_store_pitchers(sd_str, ed_str)
            bat_res = sf.fetch_and_store_batters(sd_str, ed_str)

            # 3) Team data
            team_res = []
            for season in seasons:
                team_res.append(TeamFetcher().fetch_and_store_team_batting(season))

            results = {
                "boxscores":           box_res,
                "statcast_pitchers":   pit_res,
                "statcast_batters":    bat_res,
                "team_stats":          team_res
            }

            success = all(r.get("success", False) for r in results.values())
            code    = 200 if success else 206
            msg     = (
                "Historical backfill complete"
                if success
                else f"Partial backfill; failures in {[k for k,v in results.items() if not v.get('success')]}"
            )
            return lambda_response(code, msg, results)

        # ─────────────── HISTORICAL AGGREGATIONS ───────────────
        elif mode == "historical_agg":
            if not (sd_str and ed_str):
                return lambda_response(400, "historical_agg requires start_date and end_date")

            agg_res = PitcherAggregator.aggregate_period(sd_str, ed_str)
            success = agg_res.get("success", False)
            code    = 200 if success else 206
            msg     = (
                "Historical aggregations complete"
                if success
                else "Historical aggregations encountered errors"
            )
            return lambda_response(code, msg, {"pitcher_aggs": agg_res})

        # ─────────────── DAILY UPDATE ───────────────
        elif mode == "daily_update":
            # default to yesterday if no target_date
            if tgt_str:
                tgt_date = datetime.strptime(tgt_str, "%Y-%m-%d").date()
            else:
                tgt_date = date.today() - timedelta(days=1)
            tgt_iso = tgt_date.strftime("%Y-%m-%d")

            # skip outside season
            season_bounds = get_season_date_ranges().get(tgt_date.year)
            if season_bounds:
                start_s, end_s = season_bounds
                if not (start_s <= tgt_date <= end_s):
                    return lambda_response(200, f"Date {tgt_iso} outside season—no action")

            # 1) Boxscores
            box_res = fetch_boxscores_for_date(tgt_iso)

            # 2) Statcast
            sf      = StatcastFetcher()
            pit_res = sf.fetch_and_store_pitchers(tgt_iso, tgt_iso)
            bat_res = sf.fetch_and_store_batters(tgt_iso, tgt_iso)

            # 3) Team data
            team_res = fetch_team_stats_for_date(tgt_iso)

            # 4) Daily aggregations
            agg_res = aggregate_starting_pitchers_for_date(tgt_iso, tgt_iso)

            results = {
                "boxscores":         box_res,
                "statcast_pitchers": pit_res,
                "statcast_batters":  bat_res,
                "team_stats":        team_res,
                "pitcher_aggs":      agg_res
            }

            success = all(r.get("success", True) for r in results.values())
            code    = 200 if success else 206
            msg     = (
                f"Daily update for {tgt_iso} complete"
                if success
                else f"Partial daily update for {tgt_iso}"
            )
            return lambda_response(code, msg, results)

        # ─────────────── HEALTH CHECK ───────────────
        elif mode == "health_check":
            # DB connectivity
            try:
                with RDSConnection():
                    db_ok = True
            except:
                db_ok = False

            # MLB API ping
            try:
                import requests
                r = requests.get(
                    "https://statsapi.mlb.com/api/v1/schedule"
                    "?sportId=1&startDate=2025-01-01&endDate=2025-01-01",
                    timeout=5
                )
                api_ok = (r.status_code == 200)
            except:
                api_ok = False

            status = {"db_connection": db_ok, "mlb_api": api_ok}
            code   = 200 if all(status.values()) else 500
            return lambda_response(code, "health_check", status)

        # ─────────────── UNKNOWN MODE ───────────────
        else:
            return lambda_response(400, f"Unknown mode: {mode}")

    except Exception as e:
        logger.error("Unhandled error in lambda_handler", exc_info=e)
        return lambda_response(500, f"Internal error: {str(e)}")