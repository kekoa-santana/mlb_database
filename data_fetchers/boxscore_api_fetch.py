# src/data_fetchers/boxscore_api_fetcher.py

import asyncio
import aiohttp
import pandas as pd
from lambda_utils import RDSConnection, store_dataframe_to_rds, logger
from typing import List, Dict

# —— CONFIGURATION —— #
API_BASE = "https://statsapi.mlb.com/api/v1"
LEAGUE_ID = 103  # MLB
MAX_CONCURRENT = 5
RETRY_LIMIT = 3

# target table in your RDS schema
TABLE_NAME = "mlb_boxscores"


async def fetch_game_pk(session: aiohttp.ClientSession, date_str: str) -> List[int]:
    """Get all gamePk values for a given date (YYYY-MM-DD)."""
    url = f"{API_BASE}/schedule?sportId={LEAGUE_ID}&date={date_str}"
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            async with session.get(url, timeout=10) as r:
                r.raise_for_status()
                data = await r.json()
                return [game["gamePk"] for d in data["dates"] for game in d["games"]]
        except Exception as e:
            logger.warning(f"[fetch_game_pk] attempt {attempt} failed: {e}")
            await asyncio.sleep(attempt * 2)
    raise RuntimeError(f"Failed to fetch schedule for {date_str}")


async def fetch_boxscore(session: aiohttp.ClientSession, game_pk: int) -> Dict:
    """Fetch the raw boxscore JSON for a single gamePk."""
    url = f"{API_BASE}/{game_pk}/boxscore"
    for attempt in range(1, RETRY_LIMIT + 1):
        try:
            async with session.get(url, timeout=10) as r:
                r.raise_for_status()
                return await r.json()
        except Exception as e:
            logger.warning(f"[fetch_boxscore:{game_pk}] attempt {attempt} failed: {e}")
            await asyncio.sleep(attempt * 2)
    logger.error(f"Exceeded retries for boxscore {game_pk}")
    return {}


def parse_boxscore(raw: Dict) -> pd.DataFrame:
    """
    Turn a boxscore JSON into a flat DataFrame with both batting,
    pitching, linescore, and game‐meta fields.
    """
    rows = []

    # Game‐level meta
    game = raw.get("gameData", {}).get("game", {})
    venue = raw.get("gameData", {}).get("venue", {})
    datetime_meta = raw.get("gameData", {}).get("datetime", {})
    linescore = raw.get("liveData", {}).get("linescore", {})
    box = raw.get("liveData", {}).get("boxscore", {})
    info = box.get("info", {})

    for side in ("home", "away"):
        team_meta = raw["gameData"]["teams"][side]
        stats = box.get("teams", {}).get(side, {}).get("teamStats", {})
        bat = stats.get("batting", {})
        pit = stats.get("pitching", {})

        row = {
            # identifiers
            "game_pk":            game.get("pk"),
            "date":               datetime_meta.get("officialDate"),
            "team_id":            team_meta.get("id"),
            "team_name":          team_meta.get("name"),

            # batting
            "runs":               bat.get("runs"),
            "hits":               bat.get("hits"),
            "errors":             bat.get("errors"),
            "left_on_base":       bat.get("leftOnBase"),
            "at_bats":            bat.get("atBats"),
            "doubles":            bat.get("doubles"),
            "triples":            bat.get("triples"),
            "home_runs":          bat.get("homeRuns"),
            "rbi":                bat.get("rbi"),
            "walks":              bat.get("baseOnBalls"),
            "hit_by_pitch":       bat.get("hitByPitch"),
            "strikeouts":         bat.get("strikeOuts"),
            "stolen_bases":       bat.get("stolenBases"),
            "caught_stealing":    bat.get("caughtStealing"),
            "grounded_into_dp":   bat.get("groundIntoDoublePlays"),

            # pitching
            "innings_pitched":      pit.get("inningsPitched"),
            "earned_runs_allowed":  pit.get("earnedRuns"),
            "hits_allowed":         pit.get("hits"),
            "hr_allowed":           pit.get("homeRuns"),
            "walks_allowed":        pit.get("baseOnBalls"),
            "strikeouts_pitched":   pit.get("strikeOuts"),

            # linescore & context
            "total_innings":      linescore.get("inningsPlayed"),
            "weather":            linescore.get("weather"),
            "attendance":         info.get("attendance"),
            "duration_ms":        linescore.get("gameDurationMillis"),

            # game meta
            "game_type":          game.get("type"),       # "R" regular, "P" playoff
            "day_night":          game.get("dayNight"),
            "venue_id":           venue.get("id"),
            "venue_name":         venue.get("name"),
        }

        rows.append(row)

    return pd.DataFrame(rows)


async def fetch_and_parse(date_str: str) -> pd.DataFrame:
    """Master coroutine: fetch all games for a date, download boxscores and parse."""
    conn = aiohttp.TCPConnector(limit_per_host=MAX_CONCURRENT)
    async with aiohttp.ClientSession(connector=conn) as session:
        pks = await fetch_game_pk(session, date_str)
        tasks = [fetch_boxscore(session, pk) for pk in pks]
        raws = await asyncio.gather(*tasks)
    # parse and concat all non-empty responses
    dfs = [parse_boxscore(r) for r in raws if r]
    return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()


def run_for_date(date_str: str):
    """Sync entrypoint: fetch → parse → store into RDS."""
    logger.info(f"Boxscore fetch for {date_str} starting")
    df = asyncio.run(fetch_and_parse(date_str))
    if df.empty:
        logger.warning(f"No boxscores for {date_str}")
        return {"statusCode": 204, "body": "No data"}
    # write to Postgres
    with RDSConnection() as engine:
        store_dataframe_to_rds(df, TABLE_NAME, engine=engine)
    logger.info(f"Stored {len(df)} rows into {TABLE_NAME}")
    return {"statusCode": 200, "body": f"{len(df)} records"}


# optional CLI fallback
if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Usage: python boxscore_api_fetcher.py YYYY-MM-DD")
        sys.exit(1)
    print(run_for_date(sys.argv[1]))
