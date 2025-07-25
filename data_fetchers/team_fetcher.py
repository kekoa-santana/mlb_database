import pandas as pd
import requests
from datetime import datetime, timedelta
from lambda_utils import store_dataframe_to_rds
from data_fetchers.boxscore_api_fetch import API_BASE, LEAGUE_ID
import logging

logger = logging.getLogger(__name__)

class TeamFetcher:
    """
    Fetches and stores team-level data: season batting and probable pitchers/lineups.
    """

    def fetch_and_store_team_batting(self, season: int) -> dict:
        """
        Fetch season-long team batting statistics via pybaseball and store as a table.
        Returns dict with success status and rows written.
        """
        try:
            df = pd.DataFrame()
            # Use pybaseball for team batting
            import pybaseball as pb
            df = pb.team_batting(season)
            df['season'] = season
            df['scraped_timestamp'] = datetime.utcnow()

            rows = store_dataframe_to_rds(
                df,
                table_name="season_team_batting",
                if_exists="replace"
            )
            logger.info(f"Stored {rows} rows to season_team_batting for {season}")
            return {"success": True, "rows": rows}

        except Exception as e:
            logger.error(f"Error fetching team batting for {season}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}

    def fetch_and_store_probable_lineups_for_period(self, start_date: str, end_date: str) -> dict:
        """
        Fetch probable pitchers and lineups for each date in the range
        via MLB Stats API, and store in 'probable_lineup' table.
        """
        all_rows = []
        try:
            start = datetime.fromisoformat(start_date)
            end = datetime.fromisoformat(end_date)
            current = start
            while current <= end:
                date_str = current.strftime("%Y-%m-%d")
                logger.info(f"Fetching probable lineups for {date_str}")
                # Build schedule URL with hydrate
                url = (
                    f"{API_BASE}/schedule"
                    f"?sportId={LEAGUE_ID}"
                    f"&date={date_str}"
                    f"&hydrate=probablePitcher,teamInfo(lineup)"
                )
                resp = requests.get(url, timeout=10)
                resp.raise_for_status()
                sched = resp.json().get('dates', [])
                for day in sched:
                    for game in day.get('games', []):
                        pk = game.get('gamePk')
                        home = game['teams']['home']
                        away = game['teams']['away']
                        for side in ('home', 'away'):
                            t = game['teams'][side]
                            team_abbr = t['team'].get('abbreviation')
                            prob = t.get('probablePitcher') or {}
                            lineup = t.get('lineup', {}).get('batters', [])
                            row = {
                                'game_pk': pk,
                                'game_date': date_str,
                                'team': team_abbr,
                                'probable_pitcher_id': prob.get('id'),
                                'probable_pitcher_name': prob.get('fullName'),
                                'projected_lineup': lineup,
                                'scraped_timestamp': datetime.utcnow()
                            }
                            all_rows.append(row)
                current += timedelta(days=1)

            if not all_rows:
                logger.info("No probable lineup data for period")
                return {"success": True, "rows": 0}

            df = pd.DataFrame(all_rows)
            rows = store_dataframe_to_rds(
                df,
                table_name="probable_lineup",
                if_exists="append"
            )
            logger.info(f"Stored {rows} rows to probable_lineup for {start_date}–{end_date}")
            return {"success": True, "rows": rows}

        except Exception as e:
            logger.error(f"Error fetching probable lineups for period {start_date}–{end_date}: {e}", exc_info=True)
            return {"success": False, "error": str(e)}
