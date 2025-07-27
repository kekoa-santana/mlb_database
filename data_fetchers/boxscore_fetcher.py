# data_fetchers/boxscore_fetcher.py

import requests
import pandas as pd
from datetime import datetime, timedelta

from lambda_utils import store_dataframe_to_rds, logger
from data_fetchers.boxscore_api_fetch import fetch_and_parse as _api_fetch

def fetch_boxscores_for_range(start_date: str, end_date: str) -> dict:
    """
    Backfill boxscores between start_date and end_date (inclusive).
    Returns {"success": bool, "rows": total_rows, "error_day": opt}.
    """
    sd = datetime.strptime(start_date, "%Y-%m-%d").date()
    ed = datetime.strptime(end_date,   "%Y-%m-%d").date()
    total_rows = 0

    for offset in range((ed - sd).days + 1):
        day = (sd + timedelta(days=offset)).strftime("%Y-%m-%d")

        try:
            # pull raw DataFrame for that day
            df = _api_fetch(day)
            # if empty, skip storing
            if df.empty:
                continue

            # append into RDS, count rows written
            rows = store_dataframe_to_rds(df, "mlb_boxscores", if_exists="append")
            total_rows += rows

        except Exception as e:
            logger.error(f"Failed to fetch/store boxscores for {day}", exc_info=e)
            return {
                "success": False,
                "rows": total_rows,
                "error_day": day,
                "error": str(e)
            }

    return {"success": True, "rows": total_rows}


def fetch_boxscores_for_date(date_str: str) -> dict:
    """
    Wrapper for daily updates — fetches just one day.
    """
    return fetch_boxscores_for_range(date_str, date_str)