import os
import logging
from contextlib import contextmanager
import psycopg2
import pandas as pd
import json
from datetime import datetime
from sqlalchemy import create_engine
from sqlalchemy.pool import NullPool

# Configure logging for Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

class RDSConnection:
    """Context manager for PostgreSQL RDS connections in Lambda."""
    def __init__(self):
        self.host = os.environ['RDS_HOST']
        self.database = os.environ['RDS_DATABASE']
        self.username = os.environ['RDS_USERNAME']
        self.password = os.environ['RDS_PASSWORD']
        self.port = os.environ.get('RDS_PORT', '5432')

    def __enter__(self):
        logger.info(f"⏳ Opening DB connection to {self.host}:{self.port}/{self.database} as {self.username}")
        self.conn = psycopg2.connect(
            host=self.host,
            dbname=self.database,
            user=self.username,
            password=self.password,
            port=self.port,
            connect_timeout=30
        )
        logger.info("✅ DB connection established")
        return self.conn

    def __exit__(self, exc_type, exc_val, exc_tb):
        if exc_type:
            logger.error(f"Error during DB operation: {exc_val}")
            try:
                self.conn.rollback()
            except Exception:
                pass
        logger.info("✖ Closing DB connection")
        try:
            self.conn.close()
        except Exception:
            pass

    def get_sqlalchemy_engine(self):
        """Create a SQLAlchemy engine for writes."""
        uri = f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"
        return create_engine(uri, poolclass=NullPool)


def store_dataframe_to_rds(df: pd.DataFrame,
                          table_name: str,
                          if_exists: str = 'append',
                          date_col: str = 'game_date') -> bool:
    """Store DataFrame to RDS, skipping rows already present via pre-filtering."""
    if df.empty:
        logger.info(f"Empty DataFrame for {table_name}, skipping")
        return True

    # Primary key columns per table
    pk_map = {
        'statcast_pitchers':       ['game_pk','pitcher','batter','pitch_number','at_bat_number'],
        'statcast_batters':        ['game_pk','pitcher','batter','pitch_number','at_bat_number'],
        'mlb_boxscores':           ['game_pk'],
        'season_team_batting':     ['game_pk'],
        # add others as necessary
    }
    pk_cols = pk_map.get(table_name)

    rds = RDSConnection()
    # Pre-filter existing rows if possible
    if pk_cols and date_col in df.columns:
        start = df[date_col].min().strftime('%Y-%m-%d')
        end = df[date_col].max().strftime('%Y-%m-%d')
        sql = f"SELECT {', '.join(pk_cols)} FROM {table_name} WHERE {date_col} BETWEEN '{start}' AND '{end}'"
        with rds as conn:
            existing = pd.read_sql(sql, conn)
        merged = df.merge(existing, on=pk_cols, how='left', indicator=True)
        new_df = merged[merged['_merge']=='left_only'].drop(columns=['_merge'] + pk_cols)
    else:
        new_df = df.copy()

    if new_df.empty:
        logger.info(f"No new rows for {table_name} (all duplicates or missing date_col)")
        return True

    # Write new rows
    engine = rds.get_sqlalchemy_engine()
    num_cols = len(new_df.columns)
    SQLITE_MAX_VARS = 30000
    chunksize = max(1, min(SQLITE_MAX_VARS // num_cols, 1000))

    logger.info(f"Storing {len(new_df)} NEW rows to {table_name} (chunksize={chunksize})")
    new_df.to_sql(
        name=table_name,
        con=engine,
        if_exists=if_exists,
        index=False,
        chunksize=chunksize,
        method='multi'
    )
    logger.info(f"Successfully stored {len(new_df)} rows to {table_name}")
    return True


def get_season_date_ranges() -> dict:
    """Return start/end dates for MLB seasons (regular season)."""
    from datetime import date
    return {
        2021: (date(2021,4,1),  date(2021,10,3)),
        2022: (date(2022,4,7),  date(2022,10,5)),
        2023: (date(2023,3,30), date(2023,10,1)),
        2024: (date(2024,3,28), date(2024,9,29)),
        2025: (date(2025,3,27), date(2025,9,28))
    }

def create_database_tables():
    """Create all required tables in PostgreSQL"""
    table_schemas = {
        'statcast_pitchers': '''
            CREATE TABLE IF NOT EXISTS statcast_pitchers (
                pitch_type VARCHAR(10),
                game_date DATE,
                release_speed FLOAT,
                release_pos_x FLOAT,
                release_pos_z FLOAT,
                player_name VARCHAR(100),
                batter INTEGER,
                pitcher INTEGER,
                events VARCHAR(50),
                description VARCHAR(100),
                zone INTEGER,
                game_type VARCHAR(10),
                stand VARCHAR(5),
                p_throws VARCHAR(5),
                home_team VARCHAR(10),
                away_team VARCHAR(10),
                type VARCHAR(10),
                hit_location INTEGER,
                bb_type VARCHAR(20),
                balls INTEGER,
                strikes INTEGER,
                game_year INTEGER,
                pfx_x FLOAT,
                pfx_z FLOAT,
                plate_x FLOAT,
                plate_z FLOAT,
                on_3b INTEGER,
                on_2b INTEGER,
                on_1b INTEGER,
                outs_when_up INTEGER,
                inning INTEGER,
                inning_topbot VARCHAR(10),
                at_bat_number INTEGER,
                pitch_number INTEGER,
                hc_x FLOAT,
                hc_y FLOAT,
                vx0 FLOAT,
                vy0 FLOAT,
                vz0 FLOAT,
                ax FLOAT,
                ay FLOAT,
                az FLOAT,
                sz_top FLOAT,
                sz_bot FLOAT,
                hit_distance_sc FLOAT,
                launch_speed FLOAT,
                launch_angle FLOAT,
                effective_speed FLOAT,
                release_spin_rate FLOAT,
                release_extension FLOAT,
                game_pk BIGINT,
                fielder_2 INTEGER,
                fielder_3 INTEGER,
                fielder_4 INTEGER,
                fielder_5 INTEGER,
                fielder_6 INTEGER,
                fielder_7 INTEGER,
                fielder_8 INTEGER,
                fielder_9 INTEGER,
                release_pos_y FLOAT,
                estimated_ba_using_speedangle FLOAT,
                estimated_woba_using_speedangle FLOAT,
                woba_value FLOAT,
                woba_denom FLOAT,
                babip_value FLOAT,
                iso_value FLOAT,
                launch_speed_angle INTEGER,
                pitch_name VARCHAR(50),
                home_score INTEGER,
                away_score INTEGER,
                post_away_score INTEGER,
                post_home_score INTEGER,
                if_fielding_alignment VARCHAR(20),
                of_fielding_alignment VARCHAR(20),
                spin_axis FLOAT,
                delta_home_win_exp FLOAT,
                delta_run_exp FLOAT,
                -- Added columns (not from pybaseball directly)
                pitcher_id INTEGER,
                season INTEGER,
                PRIMARY KEY (game_pk, pitcher, batter, pitch_number, at_bat_number)
            );
        ''',
        
        'statcast_batters': '''
            CREATE TABLE IF NOT EXISTS statcast_batters (
                pitch_type VARCHAR(10),
                game_date DATE,
                release_speed FLOAT,
                release_pos_x FLOAT,
                release_pos_z FLOAT,
                player_name VARCHAR(100),
                batter INTEGER,
                pitcher INTEGER,
                events VARCHAR(50),
                description VARCHAR(100),
                spin_dir FLOAT,
                spin_rate_deprecated FLOAT,
                break_angle_deprecated FLOAT,
                break_length_deprecated FLOAT,
                zone INTEGER,
                des TEXT,
                game_type VARCHAR(10),
                stand VARCHAR(5),
                p_throws VARCHAR(5),
                home_team VARCHAR(10),
                away_team VARCHAR(10),
                type VARCHAR(10),
                hit_location INTEGER,
                bb_type VARCHAR(20),
                balls INTEGER,
                strikes INTEGER,
                game_year INTEGER,
                pfx_x FLOAT,
                pfx_z FLOAT,
                plate_x FLOAT,
                plate_z FLOAT,
                on_3b INTEGER,
                on_2b INTEGER,
                on_1b INTEGER,
                outs_when_up INTEGER,
                inning INTEGER,
                inning_topbot VARCHAR(10),
                hc_x FLOAT,
                hc_y FLOAT,
                tfs_deprecated VARCHAR(50),
                tfs_zulu_deprecated VARCHAR(50),
                umpire VARCHAR(100),
                sv_id VARCHAR(50),
                vx0 FLOAT,
                vy0 FLOAT,
                vz0 FLOAT,
                ax FLOAT,
                ay FLOAT,
                az FLOAT,
                sz_top FLOAT,
                sz_bot FLOAT,
                hit_distance_sc FLOAT,
                launch_speed FLOAT,
                launch_angle FLOAT,
                effective_speed FLOAT,
                release_spin_rate FLOAT,
                release_extension FLOAT,
                game_pk BIGINT,
                fielder_2 INTEGER,
                fielder_3 INTEGER,
                fielder_4 INTEGER,
                fielder_5 INTEGER,
                fielder_6 INTEGER,
                fielder_7 INTEGER,
                fielder_8 INTEGER,
                fielder_9 INTEGER,
                release_pos_y FLOAT,
                estimated_ba_using_speedangle FLOAT,
                estimated_woba_using_speedangle FLOAT,
                woba_value FLOAT,
                woba_denom FLOAT,
                babip_value FLOAT,
                iso_value FLOAT,
                launch_speed_angle INTEGER,
                at_bat_number INTEGER,
                pitch_number INTEGER,
                pitch_name VARCHAR(50),
                home_score INTEGER,
                away_score INTEGER,
                bat_score INTEGER,
                fld_score INTEGER,
                post_away_score INTEGER,
                post_home_score INTEGER,
                post_bat_score INTEGER,
                post_fld_score INTEGER,
                if_fielding_alignment VARCHAR(20),
                of_fielding_alignment VARCHAR(20),
                spin_axis FLOAT,
                delta_home_win_exp FLOAT,
                delta_run_exp FLOAT,
                bat_speed FLOAT,
                swing_length FLOAT,
                estimated_slg_using_speedangle FLOAT,
                delta_pitcher_run_exp FLOAT,
                hyper_speed FLOAT,
                home_score_diff INTEGER,
                bat_score_diff INTEGER,
                home_win_exp FLOAT,
                bat_win_exp FLOAT,
                age_pit_legacy FLOAT,
                age_bat_legacy FLOAT,
                age_pit FLOAT,
                age_bat FLOAT,
                n_thruorder_pitcher INTEGER,
                n_priorpa_thisgame_player_at_bat INTEGER,
                pitcher_days_since_prev_game INTEGER,
                batter_days_since_prev_game INTEGER,
                pitcher_days_until_next_game INTEGER,
                batter_days_until_next_game INTEGER,
                api_break_z_with_gravity FLOAT,
                api_break_x_arm FLOAT,
                api_break_x_batter_in FLOAT,
                arm_angle FLOAT,
                season INTEGER,
                PRIMARY KEY (game_pk, pitcher, batter, pitch_number, at_bat_number)
            );
        ''',
        
        'mlb_boxscores': '''
            CREATE TABLE IF NOT EXISTS mlb_boxscores (
                game_pk BIGINT PRIMARY KEY,
                game_date DATE,
                away_team VARCHAR(10),
                home_team VARCHAR(10),
                game_number INTEGER,
                double_header VARCHAR(10),
                away_pitcher_ids TEXT,
                home_pitcher_ids TEXT,
                away_starting_pitcher_id INTEGER,
                home_starting_pitcher_id INTEGER,
                hp_umpire VARCHAR(100),
                umpire_1b VARCHAR(100),
                umpire_2b VARCHAR(100), 
                umpire_3b VARCHAR(100),
                weather VARCHAR(100),
                temp FLOAT,
                wind VARCHAR(100),
                elevation FLOAT,
                day_night VARCHAR(10),
                first_pitch VARCHAR(20),
                scraped_timestamp TIMESTAMP
            );
        ''',
        
        'game_level_starting_pitchers': '''
            CREATE TABLE IF NOT EXISTS game_level_starting_pitchers (
                game_pk BIGINT,
                game_date DATE,
                pitcher_id INTEGER,
                pitcher_hand VARCHAR(5),
                pitching_team VARCHAR(10),
                opponent_team VARCHAR(10),
                pitches INTEGER,
                innings_pitched FLOAT,
                batters_faced INTEGER,
                strikeouts INTEGER,
                swinging_strike_rate FLOAT,
                first_pitch_strike_rate FLOAT,
                csw_pct FLOAT,
                fastball_pct FLOAT,
                slider_pct FLOAT,
                curve_pct FLOAT,
                changeup_pct FLOAT,
                cutter_pct FLOAT,
                sinker_pct FLOAT,
                splitter_pct FLOAT,
                fastball_whiff_rate FLOAT,
                slider_whiff_rate FLOAT,
                curve_whiff_rate FLOAT,
                changeup_whiff_rate FLOAT,
                cutter_whiff_rate FLOAT,
                sinker_whiff_rate FLOAT,
                splitter_whiff_rate FLOAT,
                fastball_then_breaking_rate FLOAT,
                offspeed_to_fastball_ratio FLOAT,
                avg_release_speed FLOAT,
                max_release_speed FLOAT,
                avg_spin_rate FLOAT,
                unique_pitch_types INTEGER,
                zone_pct FLOAT,
                chase_rate FLOAT,
                avg_launch_speed FLOAT,
                max_launch_speed FLOAT,
                avg_launch_angle FLOAT,
                max_launch_angle FLOAT,
                hard_hit_rate FLOAT,
                barrel_rate FLOAT,
                pfx_x FLOAT,
                pfx_z FLOAT,
                release_extension FLOAT,
                release_height FLOAT,
                plate_x FLOAT,
                plate_z FLOAT,
                fip FLOAT,
                two_strike_k_rate FLOAT,
                high_leverage_k_rate FLOAT,
                woba_runners_on FLOAT,
                PRIMARY KEY (game_pk, pitcher_id)
            );
        ''',
        
        'game_level_relief_pitchers': '''
            CREATE TABLE IF NOT EXISTS game_level_relief_pitchers (
                game_pk BIGINT,
                game_date DATE,
                pitcher_id INTEGER,
                pitcher_hand VARCHAR(5),
                pitching_team VARCHAR(10),
                opponent_team VARCHAR(10),
                innings_pitched FLOAT,
                pitches INTEGER,
                batters_faced INTEGER,
                strikeouts INTEGER,
                swinging_strike_rate FLOAT,
                first_pitch_strike_rate FLOAT,
                csw_pct FLOAT,
                relief_role VARCHAR(20),
                entry_inning INTEGER,
                inherited_runners INTEGER,
                inherited_runners_scored INTEGER,
                leverage_index FLOAT,
                fastball_pct FLOAT,
                slider_pct FLOAT,
                curve_pct FLOAT,
                changeup_pct FLOAT,
                cutter_pct FLOAT,
                sinker_pct FLOAT,
                splitter_pct FLOAT,
                fastball_whiff_rate FLOAT,
                slider_whiff_rate FLOAT,
                curve_whiff_rate FLOAT,
                changeup_whiff_rate FLOAT,
                avg_release_speed FLOAT,
                avg_spin_rate FLOAT,
                zone_pct FLOAT,
                chase_rate FLOAT,
                fip FLOAT,
                PRIMARY KEY (game_pk, pitcher_id)
            );
        ''',
        
        'game_level_team_batting': '''
            CREATE TABLE IF NOT EXISTS game_level_team_batting (
                game_pk BIGINT,
                pitching_team VARCHAR(10),
                opponent_team VARCHAR(10),
                game_date DATE,
                bat_plate_appearances INTEGER,
                bat_at_bats INTEGER,
                bat_pitches INTEGER,
                bat_swings INTEGER,
                bat_whiffs INTEGER,
                bat_whiff_rate FLOAT,
                bat_called_strike_rate FLOAT,
                bat_strikeouts INTEGER,
                bat_strikeout_rate FLOAT,
                bat_strikeout_rate_behind FLOAT,
                bat_strikeout_rate_ahead FLOAT,
                bat_hits INTEGER,
                bat_singles INTEGER,
                bat_doubles INTEGER,
                bat_triples INTEGER,
                bat_home_runs INTEGER,
                bat_walks INTEGER,
                bat_hbp INTEGER,
                bat_avg FLOAT,
                bat_obp FLOAT,
                bat_slugging FLOAT,
                bat_ops FLOAT,
                bat_woba FLOAT,
                bat_L_plate_appearances INTEGER,
                bat_L_strikeout_rate FLOAT,
                bat_L_ops FLOAT,
                bat_R_plate_appearances INTEGER,
                bat_R_strikeout_rate FLOAT,
                bat_R_ops FLOAT,
                PRIMARY KEY (game_pk, opponent_team)
            );
        ''',
        
        'game_starting_lineups': '''
            CREATE TABLE IF NOT EXISTS game_starting_lineups (
                game_pk BIGINT,
                team VARCHAR(10),
                batter_id INTEGER,
                batting_order INTEGER,
                stand VARCHAR(5),
                catcher_id INTEGER,
                game_date DATE,
                PRIMARY KEY (game_pk, team, batting_order)
            );
        ''',
        
        'game_level_batters_vs_starters': '''
            CREATE TABLE IF NOT EXISTS game_level_batters_vs_starters (
                game_pk BIGINT,
                batter_id INTEGER,
                pitcher_id INTEGER,
                stand VARCHAR(5),
                pitching_team VARCHAR(10),
                opponent_team VARCHAR(10),
                game_date DATE,
                plate_appearances INTEGER,
                at_bats INTEGER,
                pitches INTEGER,
                swings INTEGER,
                whiffs INTEGER,
                whiff_rate FLOAT,
                called_strike_rate FLOAT,
                strikeouts INTEGER,
                strikeout_rate FLOAT,
                strikeout_rate_behind FLOAT,
                strikeout_rate_ahead FLOAT,
                hits INTEGER,
                singles INTEGER,
                doubles INTEGER,
                triples INTEGER,
                home_runs INTEGER,
                walks INTEGER,
                hbp INTEGER,
                two_strike_k_rate FLOAT,
                high_leverage_k_rate FLOAT,
                woba_runners_on FLOAT,
                avg FLOAT,
                obp FLOAT,
                slugging FLOAT,
                ops FLOAT,
                woba FLOAT,
                PRIMARY KEY (game_pk, batter_id, pitcher_id)
            );
        ''',
        
        'probable_lineup': '''
            CREATE TABLE IF NOT EXISTS probable_lineup (
                game_pk BIGINT,
                game_date DATE,
                team VARCHAR(10),
                home_probable_pitcher_id INTEGER,
                home_probable_pitcher_name VARCHAR(100),
                away_probable_pitcher_id INTEGER, 
                away_probable_pitcher_name VARCHAR(100),
                scraped_timestamp TIMESTAMP,
                PRIMARY KEY (game_pk, team)
            );
        ''',
        
        'player_injury_log': '''
            CREATE TABLE IF NOT EXISTS player_injury_log (
                player_id INTEGER,
                start_date DATE,
                end_date DATE,
                description TEXT,
                PRIMARY KEY (player_id, start_date)
            );
        '''
    }

    rds = RDSConnection()
    with rds as conn:
        cur = conn.cursor()
        for tbl, ddl in table_schemas.items():
            try:
                cur.execute(ddl)
                logger.info(f"Created/verified table: {tbl}")
            except Exception as e:
                logger.error(f"Failed to create table {tbl}: {e}")
                raise
        conn.commit()


def lambda_response(status_code: int, message: str, data: dict=None) -> dict:
    """Standard Lambda HTTP-style JSON response."""
    return {
        'statusCode': status_code,
        'body': json.dumps({'message': message, 'data': data or {}, 'timestamp': datetime.utcnow().isoformat()})
    }