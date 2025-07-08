import json
import pandas as pd
import logging
from datetime import datetime, date, timedelta
from lambda_utils import (
    create_database_tables, 
    lambda_response, 
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
            
        elif mode == 'historical_agg':
            return handle_historical_aggregations(fetcher, year, context)
            
        elif mode == 'daily_update':
            return handle_daily_update(fetcher, target_date, context)
            
        else:
            return lambda_response(400, f"Unknown mode: {mode}")
            
    except Exception as e:
        logger.error(f"Lambda execution failed: {e}", exc_info=True)
        return lambda_response(500, f"Internal error: {str(e)}")

def handle_historical_raw(fetcher: LambdaDataFetcher, year: int, context) -> dict:
    """Handle historical raw data fetching for a specific year"""
    
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
    """Handle historical aggregations for a specific year"""
    
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
    """Handle daily data updates"""
    
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
    
    # Fetch probable lineups for next day (if in season)
    next_day = target_date_obj + timedelta(days=1)
    if year in season_dates:
        start_season, end_season = season_dates[year]
        if start_season <= next_day <= end_season:
            results['probable_lineups'] = fetch_probable_lineups(next_day)
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

def fetch_probable_lineups(target_date: date) -> bool:
    """Fetch probable lineups/pitchers for the target date"""
    try:
        from lambda_utils import store_dataframe_to_rds
        
        # This would use the MLB API logic from the codebase
        # Adapting from src/data/mlb_api.py
        
        logger.info(f"Fetching probable lineups for {target_date}")
        
        # Placeholder - you'd implement the actual API call here
        # using the logic from scrape_probable_pitchers()
        
        probable_data = []  # This would be populated from API
        
        if probable_data:
            df = pd.DataFrame(probable_data)
            df['scraped_timestamp'] = datetime.utcnow()
            return store_dataframe_to_rds(df, 'probable_lineup', if_exists='append')
        else:
            logger.info(f"No probable lineups found for {target_date}")
            return True
            
    except Exception as e:
        logger.error(f"Error fetching probable lineups: {e}")
        return False