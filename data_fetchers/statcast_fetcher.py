import time
import pandas as pd
import pybaseball as pb
from lambda_utils import store_dataframe_to_rds
from utils.cleaning import clean_statcast_data

class StatcastFetcher:
    def __init__(self, max_retries: int = 3, retry_delay: float = 5.0):
        self.max_retries = max_retries
        self.retry_delay = retry_delay

    def fetch_with_retries(self, start: str, end: str) -> pd.DataFrame:
        for attempt in range(self.max_retries):
            try:
                time.sleep(1)
                df = pb.statcast(start_dt=start, end_dt=end)
                if isinstance(df, pd.DataFrame):
                    return df
            except Exception:
                time.sleep(self.retry_delay * 2**attempt)
        return pd.DataFrame()

    def fetch_and_store_pitchers(self, start: str, end: str) -> dict:
        raw = self.fetch_with_retries(start, end)
        if raw.empty:
            return {"success": True, "rows": 0}
        clean = clean_statcast_data(raw, perspective="pitcher")
        count = store_dataframe_to_rds(clean, "statcast_pitchers", if_exists="append")
        return {"success": True, "rows": count}

    def fetch_and_store_batters(self, start: str, end: str) -> dict:
        raw = self.fetch_with_retries(start, end)
        if raw.empty:
            return {"success": True, "rows": 0}
        clean = clean_statcast_data(raw, perspective="batter")
        count = store_dataframe_to_rds(clean, "statcast_batters", if_exists="append")
        return {"success": True, "rows": count}