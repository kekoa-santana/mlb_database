import pandas as pd
import pybaseball as pb
import numpy as np
import asyncio
from datetime import datetime, date, timedelta
import logging
from typing import List, Dict, Tuple, Optional
import time
import requests
from lambda_utils import (
    store_dataframe_to_rds, 
    RDSConnection,
    get_season_date_ranges,
    logger
)

from data_fetchers.boxscore_api_fetch import run_for_date, fetch_and_parse, parse_boxscore, API_BASE, LEAGUE_ID 

# Enable pybaseball cache for efficiency
try:
    pb.cache.enable()
    logger.info("Pybaseball cache enabled")
except Exception as e:
    logger.warning(f"Could not enable pybaseball cache: {e}")

class LambdaDataFetcher:
    # Adapted data fetcher for Lambda/RDS environment
    
    def __init__(self):
        self.failed_dates = []
        self.max_retries = 3
        self.retry_delay = 5
        
    def fetch_with_retries(self, fetch_function, *args, **kwargs):
        # Wrapper for pybaseball calls with retries
        for attempt in range(self.max_retries):
            try:
                time.sleep(1)  # Rate limiting
                result = fetch_function(*args, **kwargs)
                if isinstance(result, pd.DataFrame):
                    return result
                else:
                    logger.warning(f"Unexpected return type: {type(result)}")
                    continue
            except Exception as e:
                logger.warning(f"Attempt {attempt + 1}/{self.max_retries} failed: {e}")
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay * (2 ** attempt))
                else:
                    logger.error(f"All retries failed for {fetch_function.__name__}")
                    return None
        return None

    def fetch_statcast_pitchers_for_period(self, start_date: date, end_date: date) -> bool:
        # Fetch pitcher statcast data for a date range
        logger.info(f"Fetching pitcher data: {start_date} to {end_date}")
        
        try:
            # Convert date objects to strings for pybaseball
            start_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, date) else start_date
            end_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, date) else end_date
            
            # Fetch raw statcast data
            data = self.fetch_with_retries(
                pb.statcast,
                start_dt=start_str,
                end_dt=end_str
            )
            
            if data is None or data.empty:
                logger.warning(f"No pitcher data found for {start_date} to {end_date}")
                return True  # Not an error, just no games
                
            # Clean and process data
            data = self._clean_statcast_data(data, 'pitcher')
            
            # Store to RDS
            success = store_dataframe_to_rds(data, 'statcast_pitchers', if_exists='append')
            if not success:
                self.failed_dates.append(f"pitcher_{start_date}_{end_date}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error fetching pitcher data for {start_date}-{end_date}: {e}")
            self.failed_dates.append(f"pitcher_{start_date}_{end_date}")
            return False

    def fetch_statcast_batters_for_period(self, start_date: date, end_date: date) -> bool:
        # Fetch batter statcast data for a date range
        logger.info(f"Fetching batter data: {start_date} to {end_date}")
        
        try:
            # Convert date objects to strings for pybaseball
            start_str = start_date.strftime('%Y-%m-%d') if isinstance(start_date, date) else start_date
            end_str = end_date.strftime('%Y-%m-%d') if isinstance(end_date, date) else end_date
            
            # Use same statcast call but from batter perspective
            data = self.fetch_with_retries(
                pb.statcast,
                start_dt=start_str,
                end_dt=end_str
            )
            
            if data is None or data.empty:
                logger.warning(f"No batter data found for {start_date} to {end_date}")
                return True
                
            # Clean and process data
            data = self._clean_statcast_data(data, 'batter')
            
            # Store to RDS
            success = store_dataframe_to_rds(data, 'statcast_batters', if_exists='append')
            if not success:
                self.failed_dates.append(f"batter_{start_date}_{end_date}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error fetching batter data for {start_date}-{end_date}: {e}")
            self.failed_dates.append(f"batter_{start_date}_{end_date}")
            return False

    def fetch_mlb_boxscores_for_period(self, start_date: date, end_date: date) -> bool:
        #Fetch and parse boxscores for a date range using the StatsAPI.
        all_dfs: List[pd.DataFrame] = []

        current = start_date
        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            try:
                sched_url = f"{API_BASE}/schedule?sportId={LEAGUE_ID}&date={date_str}"
                resp = requests.get(sched_url, timeout=10)
                resp.raise_for_status()
                schedule = resp.json()
                game_pks = [g["gamePk"] for d in schedule.get("dates", []) for g in d.get("games", [])]

                for pk in game_pks:
                    try:
                        box_url = f"{API_BASE}/{pk}/boxscore"
                        r = requests.get(box_url, timeout=10)
                        r.raise_for_status()
                        df = parse_boxscore(r.json())
                        if not df.empty:
                            all_dfs.append(df)
                    except Exception as e:
                        logger.warning(f"Failed to fetch/parse boxscore {pk}: {e}")
            except Exception as e:
                logger.warning(f"Failed to fetch schedule for {date_str}: {e}")

            current += timedelta(days=1)

        return pd.concat(all_dfs, ignore_index=True) if all_dfs else pd.DataFrame()

    def fetch_team_season_batting(self, season: int) -> bool:
        # Fetch season-long team batting statistics via pybaseball and store.
        logger.info(f"Fetching team batting stats for {season}")

        df = self.fetch_with_retries(pb.team_batting, season)
        if df is None or df.empty:
            logger.warning(f"No team batting data for {season}")
        df['season'] = season
        df['scraped_timestamp'] = datetime.utcnow()

        return store_dataframe_to_rds(df, 'season_team_batting', if_exists='replace')

    def fetch_probable_pitchers_and_lineups(self, target_date: date) -> bool:
        # Hit MLB Stats API with hydrate to pull probable pitchers and lineups
        date_str = target_date.strftime("%Y-%m-%d")
        logger.info(f"Fetching probable pitchers/lineups for {date_str}")

        try:
            # hydrate both probablePitcher and teamInfo(lineup) if supported
            url = (
                f"{API_BASE}/schedule"
                f"?sportId={LEAGUE_ID}"
                f"&date={date_str}"
                f"&hydrate=probablePitcher,teamInfo(lineup)"
            )
            resp = requests.get(url, timeout=10)
            resp.raise_for_status()
            sched = resp.json().get('dates', [])
            rows = []
            for day in sched:
                for game in day.get('games', []):
                    pk = game['gamePk']
                    # teams block
                    home = game['teams']['home']
                    away = game['teams']['away']
                    for side in ('home', 'away'):
                        t = game['teams'][side]
                        team_abbr = t['team']['abbreviation']
                        prob = t.get('probablePitcher') or {}
                        lineup = t.get('lineup', {}).get('batters', [])
                        rows.append({
                            'game_pk': pk,
                            'game_date': date_str,
                            'team': team_abbr,
                            'home_probable_pitcher_id': home.get('probablePitcher', {}).get('id'),
                            'home_probable_pitcher_name': home.get('probablePitcher', {}).get('fullName'),
                            'away_probable_pitcher_id': away.get('probablePitcher', {}).get('id'),
                            'away_probable_pitcher_name': away.get('probablePitcher', {}).get('fullName'),
                            'projected_lineup': lineup,  # a list of batter IDs
                            'scraped_timestamp': datetime.utcnow()
                        })
            df = pd.DataFrame(rows)
            if df.empty:
                logger.info("No probable‐pitcher/lineup data returned")
                return True
            return store_dataframe_to_rds(df, 'probable_lineup', if_exists='append')
        except Exception as e:
            logger.error(f"Error in fetch_probable_pitchers_and_lineups: {e}")
            return False

    def _clean_statcast_data(self, df: pd.DataFrame, perspective: str) -> pd.DataFrame:
        # Clean and standardize statcast data
        if df.empty:
            return df
            
        # Filter to regular season only
        if 'game_type' in df.columns:
            df = df[df['game_type'] == 'R'].copy()
            
        # Add season column
        if 'game_date' in df.columns:
            df['game_date'] = pd.to_datetime(df['game_date'])
            df['season'] = df['game_date'].dt.year
            
        # Convert numeric columns
        numeric_cols = ['release_speed', 'release_spin_rate', 'launch_speed', 
                       'launch_angle', 'woba_value', 'woba_denom']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
                
        # Drop rows missing essential data
        essential_cols = ['game_pk', 'pitcher', 'batter']
        if perspective == 'pitcher':
            essential_cols.append('pitch_number')
        
        for col in essential_cols:
            if col in df.columns:
                df = df.dropna(subset=[col])
                
        # Remove duplicates
        if all(col in df.columns for col in ['game_pk', 'pitcher', 'batter', 'pitch_number']):
            df = df.drop_duplicates(subset=['game_pk', 'pitcher', 'batter', 'pitch_number'])
        
        # Add pitcher_id for pitcher table
        if perspective == 'pitcher' and 'pitcher_id' not in df.columns:
            df['pitcher_id'] = df['pitcher']
            
        # CRITICAL: Filter to only columns that exist in the database schema
        if perspective == 'pitcher':
            allowed_columns = [
                'pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',
                'player_name', 'batter', 'pitcher', 'events', 'description', 'zone',
                'game_type', 'stand', 'p_throws', 'home_team', 'away_team', 'type',
                'hit_location', 'bb_type', 'balls', 'strikes', 'game_year', 'pfx_x',
                'pfx_z', 'plate_x', 'plate_z', 'on_3b', 'on_2b', 'on_1b', 'outs_when_up',
                'inning', 'inning_topbot', 'at_bat_number', 'pitch_number', 'hc_x',
                'hc_y', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'sz_top', 'sz_bot',
                'hit_distance_sc', 'launch_speed', 'launch_angle', 'effective_speed',
                'release_spin_rate', 'release_extension', 'game_pk', 'fielder_2',
                'fielder_3', 'fielder_4', 'fielder_5', 'fielder_6', 'fielder_7',
                'fielder_8', 'fielder_9', 'release_pos_y', 'estimated_ba_using_speedangle',
                'estimated_woba_using_speedangle', 'woba_value', 'woba_denom',
                'babip_value', 'iso_value', 'launch_speed_angle', 'pitch_name',
                'home_score', 'away_score', 'post_away_score', 'post_home_score',
                'if_fielding_alignment', 'of_fielding_alignment', 'spin_axis',
                'delta_home_win_exp', 'delta_run_exp', 'pitcher_id', 'season'
            ]
        else:  # batter perspective
            allowed_columns = [
                'pitch_type', 'game_date', 'release_speed', 'release_pos_x', 'release_pos_z',
                'player_name', 'batter', 'pitcher', 'events', 'description', 'zone',
                'game_type', 'stand', 'p_throws', 'home_team', 'away_team', 'type',
                'hit_location', 'bb_type', 'balls', 'strikes', 'game_year', 'pfx_x',
                'pfx_z', 'plate_x', 'plate_z', 'on_3b', 'on_2b', 'on_1b', 'outs_when_up',
                'inning', 'inning_topbot', 'at_bat_number', 'pitch_number', 'hc_x',
                'hc_y', 'vx0', 'vy0', 'vz0', 'ax', 'ay', 'az', 'sz_top', 'sz_bot',
                'hit_distance_sc', 'launch_speed', 'launch_angle', 'effective_speed',
                'release_spin_rate', 'release_extension', 'game_pk', 'fielder_2',
                'fielder_3', 'fielder_4', 'fielder_5', 'fielder_6', 'fielder_7',
                'fielder_8', 'fielder_9', 'release_pos_y', 'estimated_ba_using_speedangle',
                'estimated_woba_using_speedangle', 'woba_value', 'woba_denom',
                'babip_value', 'iso_value', 'launch_speed_angle', 'pitch_name',
                'home_score', 'away_score', 'post_away_score', 'post_home_score',
                'if_fielding_alignment', 'of_fielding_alignment', 'spin_axis',
                'delta_home_win_exp', 'delta_run_exp', 'season'
            ]
        
        # Keep only columns that exist in both the DataFrame and our schema
        final_columns = [col for col in allowed_columns if col in df.columns]
        df = df[final_columns].copy()
        
        logger.info(f"Filtered DataFrame to {len(final_columns)} columns matching database schema")
        
        return df

    def _fetch_boxscores_simple(self, start_date: date, end_date: date) -> pd.DataFrame:
        # Simplified async boxscore fetcher using the API helpers.

        dfs: List[pd.DataFrame] = []
        current = start_date

        while current <= end_date:
            date_str = current.strftime("%Y-%m-%d")
            df = asyncio.run(fetch_and_parse(date_str))
            if not df.empty:
                df["scraped_timestamp"] = datetime.utcnow()
                dfs.append(df)
            current += timedelta(days=1)

        return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    def aggregate_starting_pitchers_for_period(self, start_date: date, end_date: date) -> bool:
        # Create game-level starting pitcher stats
        logger.info(f"Aggregating starting pitchers: {start_date} to {end_date}")
        
        try:
            rds = RDSConnection()
            with rds.get_connection() as conn:
                # Query raw pitcher data for the period
                query = """
                SELECT * FROM statcast_pitchers 
                WHERE game_date BETWEEN %s AND %s
                AND game_type = 'R'
                """
                
                df = pd.read_sql(query, conn, params=[start_date, end_date])
                
            if df.empty:
                logger.warning(f"No pitcher data to aggregate for {start_date}-{end_date}")
                return True
                
            # Adapt the aggregation logic from create_starting_pitcher_table.py
            aggregated = self._aggregate_pitcher_stats(df)
            
            success = store_dataframe_to_rds(aggregated, 'game_level_starting_pitchers', if_exists='append')
            if not success:
                self.failed_dates.append(f"agg_pitchers_{start_date}_{end_date}")
                
            return success
            
        except Exception as e:
            logger.error(f"Error aggregating pitcher data: {e}")
            self.failed_dates.append(f"agg_pitchers_{start_date}_{end_date}")
            return False

    def _aggregate_pitcher_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        # Aggregate pitch-level data to game-level stats
        # This adapts the logic from create_starting_pitcher_table.py
        
        # Filter for starting pitchers (first inning, substantial pitch count)
        starters = df.groupby(['game_pk', 'pitcher']).agg({
            'inning': ['min', 'max'],
            'pitch_number': 'count'
        }).reset_index()
        
        starters.columns = ['game_pk', 'pitcher', 'min_inning', 'max_inning', 'total_pitches']
        
        # Define starting pitcher criteria
        starter_mask = (
            (starters['min_inning'] == 1) & 
            (starters['total_pitches'] >= 30) & 
            (starters['max_inning'] >= 3)
        )
        
        starter_games = starters[starter_mask][['game_pk', 'pitcher']]
        
        # Merge back to get only starter data
        starter_data = df.merge(starter_games, on=['game_pk', 'pitcher'])

        # Per-pitch flags for strike type metrics
        starter_data['swing_miss'] = starter_data['description'].isin(
            ['swinging_strike', 'swinging_strike_blocked']
        )
        starter_data['first_strike'] = (
            (starter_data['pitch_number'] == 1) & (starter_data['type'] == 'S')
        )
        
        # Aggregate to game level
        agg_dict = {
            'game_date': 'first',
            'pitching_team': lambda x: x.iloc[0] if len(x) > 0 else None,
            'opponent_team': lambda x: x.iloc[0] if len(x) > 0 else None,
            'inning': 'nunique',  # innings pitched
            'pitch_number': 'count',  # total pitches
            'events': lambda x: (x.isin(['strikeout', 'strikeout_double_play'])).sum(),  # strikeouts
        }
        
        # Determine team assignments
        starter_data['pitching_team'] = np.where(
            starter_data['inning_topbot'] == 'Top',
            starter_data['home_team'],
            starter_data['away_team']
        )
        starter_data['opponent_team'] = np.where(
            starter_data['inning_topbot'] == 'Top', 
            starter_data['away_team'],
            starter_data['home_team']
        )
        
        result = starter_data.groupby(['game_pk', 'pitcher']).agg(agg_dict).reset_index()

        # Calculate rate metrics separately
        rate_df = (
            starter_data
            .groupby(['game_pk', 'pitcher'])[['swing_miss', 'first_strike']]
            .mean()
            .reset_index()
            .rename(columns={
                'swing_miss': 'swinging_strike_rate',
                'first_strike': 'first_pitch_strike_rate'
            })
        )

        # Merge rates before renaming columns
        result = result.merge(rate_df, on=['game_pk', 'pitcher'], how='left')
        
        # Rename columns to match schema
        result = result.rename(columns={
            'pitcher': 'pitcher_id',
            'inning': 'innings_pitched', 
            'pitch_number': 'pitches',
            'events': 'strikeouts'
        })
        
        result[['swinging_strike_rate', 'first_pitch_strike_rate']] = result[
            ['swinging_strike_rate', 'first_pitch_strike_rate']
        ].fillna(0.0)
        
        return result

    def fetch_year_data(self, year: int) -> Dict[str, bool]:
        # Fetch all data for a specific year
        logger.info(f"Starting data fetch for year {year}")
        
        date_ranges = get_season_date_ranges()
        if year not in date_ranges:
            logger.error(f"No date range defined for year {year}")
            return {'success': False}
            
        # pull the full-season team batting table
        results = {}
        results['team_batting'] = self.fetch_team_season_batting(year)
        # Break into monthly chunks to avoid timeouts

        start_date, end_date = date_ranges[year]
        current_date = start_date
        while current_date <= end_date:
            chunk_end = min(current_date + timedelta(days=30), end_date)
            
            logger.info(f"Processing chunk: {current_date} to {chunk_end}")
            
            # Fetch raw data
            results[f'pitchers_{current_date}'] = self.fetch_statcast_pitchers_for_period(current_date, chunk_end)
            results[f'batters_{current_date}'] = self.fetch_statcast_batters_for_period(current_date, chunk_end)
            results[f'boxscores_{current_date}'] = self.fetch_mlb_boxscores_for_period(current_date, chunk_end)
            
            current_date = chunk_end + timedelta(days=1)
            
        return {
            'success': all(results.values()),
            'failed_periods': [k for k, v in results.items() if not v],
            'failed_dates': self.failed_dates
        }

    def aggregate_year_data(self, year: int) -> Dict[str, bool]:
        # Run aggregations for a specific year
        logger.info(f"Starting aggregations for year {year}")
        
        date_ranges = get_season_date_ranges()
        if year not in date_ranges:
            return {'success': False}
            
        start_date, end_date = date_ranges[year]
        
        results = {}
        results['starting_pitchers'] = self.aggregate_starting_pitchers_for_period(start_date, end_date)
        # Add other aggregations here as needed
        
        return {
            'success': all(results.values()),
            'failed_aggregations': [k for k, v in results.items() if not v]
        }