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

def lambda_handler(event, context):
    """
    Main Lambda handler for MLB data pipeline
    
    Event structure:
    {
        "mode": "historical_raw" | "historical_agg" | "daily_update",
        "year": 2024,  # For historical modes
        "date": "2024-03-28"  # For daily mode
    }
    """
    
    try:
        # Parse event
        mode = event.get('mode', 'daily_update')
        year = event.get('year')
        target_date = event.get('date')
        
        logger.info(f"Lambda invoked with mode: {mode}, year: {year}, date: {target_date}")
        
        # Ensure database tables exist
        create_database_tables()
        
        # Initialize data fetcher
        fetcher = LambdaDataFetcher()
        
        if mode == 'historical_raw':
            return handle_historical_raw(fetcher, year, context)

        elif mode == "boxscores_raw":
            # Parse the YYYY-MM-DD string into a date object
            target = datetime.strptime(event["date"], "%Y-%m-%d").date()
            # Fetch boxscores for that one day
            df = fetcher._fetch_boxscores_simple(target, target)
            # Return JSON-serializable output
            return lambda_response(
                200,
                f"Fetched boxscores for {event['date']}",
                df.to_dict(orient="records")
            )
            
        elif mode == 'historical_agg':
            return handle_historical_aggregations(fetcher, year, context)
            
        elif mode == 'daily_update':
            return handle_daily_update(fetcher, target_date, context)

        elif mode == "historical_chunk":
            return handle_historical_chunk(fetcher,
                                       event["start_date"],
                                       event["end_date"])

        elif mode == "refresh_lineups_only":
            return handle_refresh_lineups_only(fetcher,
                                            event["date"])

        elif mode == "health_check":
            return handle_health_check()
        
        else:
            return lambda_response(400, f"Unknown mode: {mode}")
                
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return lambda_response(500, f"Internal error: {str(e)}")

def handle_historical_raw(fetcher: LambdaDataFetcher, year: int, context) -> dict:
    # Handle historical raw data fetching for a specific year
    
    if not year:
        return lambda_response(400, "Year parameter required for historical_raw mode")
        
    if year not in get_season_date_ranges():
        return lambda_response(400, f"Invalid year: {year}")
    
    logger.info(f"Starting historical raw data fetch for {year}")
    
    # Check remaining time
    remaining_time = context.get_remaining_time_in_millis() / 1000 if context else 900
    logger.info(f"Remaining execution time: {remaining_time}s")
    
    # Fetch data for the year
    results = fetcher.fetch_year_data(year)
    
    if results['success']:
        message = f"Successfully fetched raw data for {year}"
        logger.info(message)
        return lambda_response(200, message, results)
    else:
        message = f"Partial failure fetching raw data for {year}"
        logger.warning(f"{message}. Failed dates: {results.get('failed_dates', [])}")
        return lambda_response(206, message, results)  # 206 = Partial Content

def handle_historical_aggregations(fetcher: LambdaDataFetcher, year: int, context) -> dict:
    # Handle historical aggregations for a specific year
    
    if not year:
        return lambda_response(400, "Year parameter required for historical_agg mode")
        
    logger.info(f"Starting historical aggregations for {year}")
    
    # Run aggregations
    results = fetcher.aggregate_year_data(year)
    
    if results['success']:
        message = f"Successfully aggregated data for {year}"
        logger.info(message)
        return lambda_response(200, message, results)
    else:
        message = f"Partial failure aggregating data for {year}"
        logger.warning(f"{message}. Failed aggregations: {results.get('failed_aggregations', [])}")
        return lambda_response(206, message, results)

def handle_daily_update(fetcher: LambdaDataFetcher, target_date: str, context) -> dict:
    # Handle daily data updates
    
    # Use yesterday if no date provided
    if not target_date:
        yesterday = date.today() - timedelta(days=1)
        target_date = yesterday.strftime('%Y-%m-%d')
        
    logger.info(f"Starting daily update for {target_date}")
    
    try:
        # Convert string to date object once
        if isinstance(target_date, str):
            target_date_obj = datetime.strptime(target_date, '%Y-%m-%d').date()
        else:
            target_date_obj = target_date
    except ValueError:
        return lambda_response(400, f"Invalid date format: {target_date}. Use YYYY-MM-DD")
    
    # Check if date is in season
    year = target_date_obj.year
    season_dates = get_season_date_ranges()
    
    if year in season_dates:
        start_season, end_season = season_dates[year]
        if not (start_season <= target_date_obj <= end_season):
            logger.info(f"Date {target_date} is outside season range")
            return lambda_response(200, f"Date {target_date} outside season - no action needed")
    
    # Fetch single day data
    results = {}
    results['pitchers'] = fetcher.fetch_statcast_pitchers_for_period(target_date_obj, target_date_obj)
    results['batters'] = fetcher.fetch_statcast_batters_for_period(target_date_obj, target_date_obj)
    results['boxscores'] = fetcher.fetch_mlb_boxscores_for_period(target_date_obj, target_date_obj)
    
    # Run quick aggregations for the day
    results['agg_pitchers'] = fetcher.aggregate_starting_pitchers_for_period(target_date_obj, target_date_obj)

    # seasonal team batting stats (replace table so it's always current)
    season = target_date_obj.year
    results['team_batting'] = fetcher.fetch_team_season_batting(season)
    
    # Fetch probable lineups for next day (if in season)
    next_day = target_date_obj + timedelta(days=1)
    if year in season_dates:
        start_season, end_season = season_dates[year]
        if start_season <= next_day <= end_season:
            results['probable_lineups'] = fetcher.fetch_probable_pitchers_and_lineups(next_day)
        else:
            results['probable_lineups'] = True  # No probables needed outside season
    
    success_count = sum(1 for v in results.values() if v)
    total_count = len(results)
    
    if success_count == total_count:
        message = f"Successfully processed daily update for {target_date}"
        return lambda_response(200, message, {
            'date': target_date,
            'operations_completed': total_count,
            'failed_dates': fetcher.failed_dates
        })
    else:
        message = f"Partial success for daily update {target_date}"
        return lambda_response(206, message, {
            'date': target_date,
            'operations_completed': success_count,
            'operations_total': total_count,
            'failed_dates': fetcher.failed_dates
        })

def handle_historical_chunk(fetcher: LambdaDataFetcher, start_str: str, end_str: str) -> dict:
    # Backfill raw data just between start_date and end_date inclusively.
    start = datetime.strptime(start_str, "%Y-%m-%d").date()
    end = datetime.strptime(end_str, "%Y-%m-%d").date()

    # Raw fetches
    pitchers = fetcher.fetch_statcast_pitchers_for_period(start, end)
    batters = fetcher.fetch_statcast_batters_for_period(start, end)
    boxscores = fetcher.fetch_mlb_boxscores_for_period(start, end)

    team_stats = fetcher.fetch_team_season_batting(start.year)
    return {
        "pitchers": pitchers,
        "batters": batters,
        "boxscores": boxscores,
        "team_batting": team_stats,
    }

def handle_health_check() -> dict:
    # Very light check: can we talk to the DB and can we hit an MLB endpoint
    
    # RDS connectivity
    try: 
        conn = RDSConnection.get_connection()
        conn.close()
        db_ok=True
    except Exception as e:
        db_ok = False
    
    # MLB API ping
    try:
        r = requests.get("https://statsapi.mlb.com/api/v1/schedule?sportId=1&gamePk=1", timeout=5)
        api_ok = r.status_code == 200
    except:
        api_ok = False

    status = {
        "db_connection": db_ok,
        "mlb_api": api_ok
    }
    code = 200 if all(status.values()) else 500
    return lambda_response(code, "health_check", status)

def handle_refresh_lineups_only(fetcher: LambdaDataFetcher, date_str: str) -> dict:
    # Re-run only the probable-pitcher + lineup fetch for the given date
    target = datetime.strptime(date_str, "%Y-%m-%d").date()
    ok = fetcher.fetch_probable_pitchers_and_lineups(target)
    return lambda_response(200 if ok else 500,
                            "refresh_lineups_only",
                            {"lineups_refreshed": ok})