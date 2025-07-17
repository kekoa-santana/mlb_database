import os
# Force any "Home"-based caches into the Lambda-writeable /tmp directory
os.environ['HOME'] = '/tmp'

import pybaseball as pb
try:
    pb.cache.disable()
except Exception:
    pass

import pandas as pd
import requests
from datetime import datetime, date, timedelta
from lambda_utils import (
    create_database_tables, 
    lambda_response,
    RDSConnection,
    get_season_date_ranges,
    logger
)
from lambda_data_fetcher import LambdaDataFetcher

# Top-level Lambda handler

def lambda_handler(event, context):
    """
    Main Lambda handler for MLB data pipeline
    """
    try:
        # Parse event
        mode        = event.get('mode', 'daily_update')
        start_str   = event.get('start_date')
        end_str     = event.get('end_date')
        target_str  = event.get('target_date')
        # Parse into date objects or None
        start_date  = datetime.strptime(start_str, "%Y-%m-%d").date() if start_str else None
        end_date    = datetime.strptime(end_str,   "%Y-%m-%d").date() if end_str   else None
        target_date = datetime.strptime(target_str, "%Y-%m-%d").date() if target_str else None

        logger.info(f"Lambda invoked with mode={mode!r}, start_date={start_date!r}, end_date={end_date!r}, target_date={target_date!r}")

        # Ensure database tables exist
        create_database_tables()

        # Initialize data fetcher
        fetcher = LambdaDataFetcher()

        if mode == 'historical_raw':
            if not start_date or not end_date:
                return lambda_response(400, "historical_raw requires start_date and end_date")
            return handle_historical_raw(fetcher, start_date, end_date, context)

        elif mode == 'historical_chunk':
            if not start_date or not end_date:
                return lambda_response(400, "historical_chunk requires start_date and end_date")
            return handle_historical_chunk(fetcher, start_date, end_date)

        elif mode == 'historical_agg':
            year = event.get('year')
            return handle_historical_aggregations(fetcher, year, context)

        elif mode == 'boxscores_raw':
            # Single-day boxscores
            date_str = event.get('date')
            if not date_str:
                return lambda_response(400, "boxscores_raw requires date (YYYY-MM-DD)")
            target = datetime.strptime(date_str, "%Y-%m-%d").date()
            df = fetcher._fetch_boxscores_simple(target, target)
            return lambda_response(200, f"Fetched boxscores for {date_str}", df.to_dict(orient="records"))

        elif mode == 'daily_update':
            return handle_daily_update(fetcher, target_date, context)

        elif mode == 'refresh_lineups_only':
            date_str = event.get('date')
            if not date_str:
                return lambda_response(400, "refresh_lineups_only requires date (YYYY-MM-DD)")
            return handle_refresh_lineups_only(fetcher, date_str)

        elif mode == 'health_check':
            return handle_health_check()

        else:
            return lambda_response(400, f"Unknown mode: {mode}")

    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return lambda_response(500, f"Internal error: {str(e)}")


# Handlers


def handle_historical_raw(fetcher: LambdaDataFetcher,
                          start_date: date,
                          end_date:   date,
                          context) -> dict:
    """
    Backfill all raw data between start_date and end_date inclusive.
    """
    logger.info(f"Starting historical_raw from {start_date} to {end_date}")
    remaining = context.get_remaining_time_in_millis() / 1000 if context else None
    logger.info(f"Remaining time: {remaining}s")

    # Raw fetches
    p = fetcher.fetch_statcast_pitchers_for_period(start_date, end_date)
    b = fetcher.fetch_statcast_batters_for_period(start_date, end_date)
    x = fetcher.fetch_mlb_boxscores_for_period(start_date, end_date)

    success = all(res.get('success', False) for res in (p, b, x))
    results = {'pitchers': p, 'batters': b, 'boxscores': x}

    if success:
        return lambda_response(200, f"Raw backfill complete: {start_date}→{end_date}", results)
    else:
        failed = [k for k,v in results.items() if not v.get('success', False)]
        return lambda_response(206,
            f"Partial backfill; failures in {failed}",
            results)


def handle_historical_chunk(fetcher: LambdaDataFetcher,
                            start_date: date,
                            end_date:   date) -> dict:
    logger.info(f"Starting historical_chunk from {start_date} to {end_date}")
    p = fetcher.fetch_statcast_pitchers_for_period(start_date, end_date)
    b = fetcher.fetch_statcast_batters_for_period(start_date, end_date)
    x = fetcher.fetch_mlb_boxscores_for_period(start_date, end_date)
    team = fetcher.fetch_team_season_batting(start_date.year)
    return {'pitchers': p, 'batters': b, 'boxscores': x, 'team_batting': team}


def handle_historical_aggregations(fetcher: LambdaDataFetcher, year: int, context) -> dict:
    if not year:
        return lambda_response(400, "Year parameter required for historical_agg mode")
    logger.info(f"Starting historical aggregations for {year}")
    results = fetcher.aggregate_year_data(year)
    if results.get('success'):
        return lambda_response(200, f"Successfully aggregated data for {year}", results)
    else:
        return lambda_response(206, f"Partial failure aggregating data for {year}", results)


def handle_daily_update(fetcher: LambdaDataFetcher, target_date: date, context) -> dict:
    # Default to yesterday
    if not target_date:
        target_date = date.today() - timedelta(days=1)
    logger.info(f"Starting daily update for {target_date}")
    # Validate
    try:
        target_obj = target_date
    except Exception:
        return lambda_response(400, f"Invalid date format: {target_date}")

    # Season check
    sd = get_season_date_ranges()
    year = target_obj.year
    if year in sd:
        start_s, end_s = sd[year]
        if not (start_s <= target_obj <= end_s):
            return lambda_response(200, f"Date {target_obj} outside season - no action")
    # Fetch
    res = {}
    res['pitchers'] = fetcher.fetch_statcast_pitchers_for_period(target_obj, target_obj)
    res['batters'] = fetcher.fetch_statcast_batters_for_period(target_obj, target_obj)
    res['boxscores'] = fetcher.fetch_mlb_boxscores_for_period(target_obj, target_obj)
    res['agg_pitchers'] = fetcher.aggregate_starting_pitchers_for_period(target_obj, target_obj)
    res['team_batting'] = fetcher.fetch_team_season_batting(year)
    next_day = target_obj + timedelta(days=1)
    if year in sd and sd[year][0] <= next_day <= sd[year][1]:
        res['probable_lineups'] = fetcher.fetch_probable_pitchers_and_lineups(next_day)
    success = sum(bool(v) for v in res.values())
    if success == len(res):
        return lambda_response(200, f"Successfully processed daily update for {target_obj}", res)
    else:
        return lambda_response(206, f"Partial success for daily update {target_obj}", res)

def handle_refresh_lineups_only(fetcher: LambdaDataFetcher, date_str: str) -> dict:
    try:
        target = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        return lambda_response(400, f"Invalid date: {date_str}")
    ok = fetcher.fetch_probable_pitchers_and_lineups(target)
    return lambda_response(200 if ok else 500, "refresh_lineups_only", {"lineups_refreshed": ok})

def handle_health_check() -> dict:
    # DB
    try:
        with RDSConnection() as conn:
            pass
        db_ok = True
    except:
        db_ok = False
    # API
    try:
        r = requests.get("https://statsapi.mlb.com/api/v1/schedule?sportId=1&startDate=2025-01-01&endDate=2025-01-01", timeout=5)
        api_ok = r.status_code == 200
    except:
        api_ok = False
    status = {"db_connection": db_ok, "mlb_api": api_ok}
    code = 200 if all(status.values()) else 500
    return lambda_response(code, "health_check", status)