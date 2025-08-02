# data_fetchers/boxscore_fetcher.py

import requests
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime, timedelta
import logging

from lambda_utils import RDSConnection

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

class BoxscoreFetcher:
    BASE_URL = "https://statsapi.mlb.com/api/v1"

    @classmethod
    def _fetch_schedule(cls, date_str: str) -> pd.DataFrame:
        """
        Hits /schedule?sportId=1&date=… and returns a DataFrame with
        the keys we need for mlb_boxscores.
        """
        url = f"{cls.BASE_URL}/schedule"
        params = {"sportId": 1, "date": date_str}
        resp = requests.get(url, params=params)
        resp.raise_for_status()
        data = resp.json().get("dates", [])
        if not data:
            return pd.DataFrame(columns=[
                "game_pk", "game_date", "day_night", "double_header", "game_number"
            ])

        rows = []
        for day in data:
            for g in day.get("games", []):
                rows.append({
                    "game_pk":       g["gamePk"],
                    "game_date":     g["gameDate"],
                    "day_night":     g["dayNight"],
                    "double_header": g["doubleHeader"],
                    "game_number":   g["gameNumber"],
                })
        return pd.DataFrame(rows)

    @classmethod
    def _fetch_one_boxscore(cls, game_pk: int) -> dict:
        """
        Fetches /game/{gamePk}/boxscore and returns a flat dict of the
        remaining columns we need.
        """
        url = f"{cls.BASE_URL}/game/{game_pk}/boxscore"
        resp = requests.get(url)
        resp.raise_for_status()
        js = resp.json()
        teams     = js["teams"]
        officials = js.get("officials", [])
        misc_info = js.get("info", [])

        out = {
            "game_pk":                  game_pk,
            "venue":                    teams["home"]["team"]["venue"]["name"],
            "home_team":                teams["home"]["team"]["abbreviation"],
            "away_team":                teams["away"]["team"]["abbreviation"],
            "home_team_id":             teams["home"]["team"]["id"],
            "away_team_id":             teams["away"]["team"]["id"],
            "away_batters_ids":         teams["away"].get("batters", []),
            "home_batters_ids":         teams["home"].get("batters", []),
            "away_pitchers_ids":        teams["away"].get("pitchers", []),
            "home_pitchers_ids":        teams["home"].get("pitchers", []),
            "away_bench_ids":           teams["away"].get("bench", []),
            "home_bench_ids":           teams["home"].get("bench", []),
            "away_bullpen_ids":         teams["away"].get("bullpen", []),
            "home_bullpen_ids":         teams["home"].get("bullpen", []),
            "away_batting_order":       teams["away"].get("battingOrder", []),
            "home_batting_order":       teams["home"].get("battingOrder", []),
            "away_starting_pitcher_id": teams["away"].get("pitchers", [None])[0],
            "home_starting_pitcher_id": teams["home"].get("pitchers", [None])[0],
            "hp_umpire":                None,
            "umpire_1b":                None,
            "umpire_2b":                None,
            "umpire_3b":                None,
            "weather":                  None,
            "temp":                     None,
            "wind":                     None,
            "elevation":                None,
            "first_pitch":              None,
            "scraped_timestamp":        pd.Timestamp.utcnow()
        }

        # parse umpires
        for off in officials:
            typ  = off["officialType"]
            name = off["official"]["fullName"]
            if   typ == "Home Plate":    out["hp_umpire"] = name
            elif typ == "First Base":    out["umpire_1b"] = name
            elif typ == "Second Base":   out["umpire_2b"] = name
            elif typ == "Third Base":    out["umpire_3b"] = name

        # parse weather, wind, first pitch
        for itm in misc_info:
            lbl = itm.get("label", "")
            val = itm.get("value", "")
            if lbl == "Weather":
                t, w = val.split(",", 1)
                out["temp"]    = float(t.split()[0])
                out["weather"] = w.strip().strip(".")
            elif lbl == "Wind":
                out["wind"] = val.strip(".")
            elif lbl in ("Firstpitch", "First Pitch"):
                out["first_pitch"] = val.strip(" .")

        return out

    @classmethod
    def fetch_and_store_range(cls, start_date: str, end_date: str) -> dict:
        sd = datetime.strptime(start_date, "%Y-%m-%d").date()
        ed = datetime.strptime(end_date,   "%Y-%m-%d").date()
        total_rows = 0

        for single in (sd + timedelta(days=i) for i in range((ed - sd).days + 1)):
            ds = single.strftime("%Y-%m-%d")
            sched_df = cls._fetch_schedule(ds)
            logger.info("  • %s: %d games scheduled", ds, len(sched_df))
            if sched_df.empty:
                logger.info(f"    ↳ No games on {ds}")
                continue

            records = []
            for _, row in sched_df.iterrows():
                box = cls._fetch_one_boxscore(row["game_pk"])
                box.update({
                    "game_date":     row["game_date"],
                    "day_night":     row["day_night"],
                    "double_header": row["double_header"],
                    "game_number":   row["game_number"],
                })
                records.append(box)

            df = pd.DataFrame(records)
            logger.info("    ↳ Prepared DataFrame: rows=%d, columns=%s",
                        len(df), df.columns.tolist())
            if df.empty:
                logger.warning("      ⚠️ DataFrame is empty after boxscore parse")
                continue

            # Upsert into mlb_boxscores on game_pk
            written = cls._upsert_to_rds(df, table="mlb_boxscores", conflict_column="game_pk")
            total_rows += written
            logger.info(f"    ↳ Upserted {written} rows for {ds}")

        return {"success": True, "rows": total_rows}

    @classmethod
    def fetch_and_store_date(cls, date_str: str) -> dict:
        return cls.fetch_and_store_range(date_str, date_str)

    @staticmethod
    def _upsert_to_rds(df: pd.DataFrame, table: str, conflict_column: str) -> int:
        """
        Batch-UPSERT the DataFrame into `table` on (`conflict_column`).
        """
        # 1) turn DataFrame into list of tuples
        cols = df.columns.tolist()
        tuples = [tuple(x) for x in df.to_numpy()]

        # 2) build INSERT … ON CONFLICT … DO UPDATE SQL
        insert_sql = f"""
            INSERT INTO {table} ({', '.join(cols)})
            VALUES %s
            ON CONFLICT ({conflict_column})
            DO UPDATE SET
              {', '.join(f"{c}=EXCLUDED.{c}" for c in cols if c != conflict_column)};
        """

        # 3) open a psycopg2 connection via your RDSConnection
        with RDSConnection() as conn:
            with conn.cursor() as cur:
                execute_values(cur, insert_sql, tuples, page_size=100)
            conn.commit()

        return len(tuples)


# Lambda entry‐points
def fetch_boxscores_for_range(start_date: str, end_date: str) -> dict:
    return BoxscoreFetcher.fetch_and_store_range(start_date, end_date)

def fetch_boxscores_for_date(date_str: str) -> dict:
    return BoxscoreFetcher.fetch_and_store_date(date_str)
