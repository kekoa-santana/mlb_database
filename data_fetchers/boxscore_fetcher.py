import requests
import pandas as pd
from data_fetchers.boxscore_api_fetch import fetch_and_parse  # your existing helper
from lambda_utils import store_dataframe_to_rds

class BoxscoreFetcher:

    def fetch_and_store_boxscores(self, date_str: str) -> dict:
        df = fetch_and_parse(date_str)
        if df.empty:
            return {"success": True, "rows": 0}
        count = store_dataframe_to_rds(df, "mlb_boxscores", if_exists="append")
        return {"success": True, "rows": count}
