import pandas as pd
import logging
from lambda_utils import DB_COLUMNS

logger = logging.getLogger(__name__)

def clean_statcast_data(df: pd.DataFrame, perspective: str) -> pd.DataFrame:
    """
    Clean and standardize Statcast data for 'pitcher' or 'batter' perspective:
      - Filter to regular season
      - Parse dates, add 'season'
      - Coerce numeric columns
      - Drop rows missing essential fields
      - Remove duplicates
      - Filter to allowed schema columns
    """
    # Early exit
    if df.empty:
        return df

    # 1. Filter to regular season
    if 'game_type' in df.columns:
        df = df[df['game_type'] == 'R'].copy()

    # 2. Parse dates and season
    if 'game_date' in df.columns:
        df['game_date'] = pd.to_datetime(df['game_date'], errors='coerce')
        df['season'] = df['game_date'].dt.year

    # 3. Numeric coercion
    numeric_cols = [
        'release_speed', 'release_spin_rate',
        'launch_speed', 'launch_angle',
        'woba_value', 'woba_denom'
    ]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    # 4. Drop rows missing essential fields
    essentials = ['game_pk', 'pitcher', 'batter']
    if perspective == 'pitcher':
        essentials.append('pitch_number')
    before = len(df)
    df = df.dropna(subset=[c for c in essentials if c in df.columns])
    dropped = before - len(df)
    if dropped:
        logger.info(f"Dropped {dropped} rows missing {essentials}")

    # 5. Remove duplicates
    if perspective == 'pitcher':
        subset = ['game_pk', 'pitcher', 'batter', 'pitch_number']
    else:
        subset = ['game_pk', 'pitcher', 'batter', 'at_bat_number']
    if all(c in df.columns for c in subset):
        df = df.drop_duplicates(subset=subset)

    # 6. Add pitcher_id if needed
    if perspective == 'pitcher' and 'pitcher_id' not in df.columns:
        df['pitcher_id'] = df['pitcher']

    # 7. Schema filter
    key = 'statcast_pitchers' if perspective == 'pitcher' else 'statcast_batters'
    allowed = DB_COLUMNS.get(key, [])
    final_cols = [c for c in allowed if c in df.columns]
    df = df.loc[:, final_cols].copy()

    return df
