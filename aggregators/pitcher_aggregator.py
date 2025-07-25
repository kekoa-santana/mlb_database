import pandas as pd
import numpy as np
from lambda_utils import store_dataframe_to_rds, RDSConnection

class PitcherAggregator:
    """
    Aggregates pitch-level statcast data into game-level starting pitcher metrics.
    """

    def aggregate_period(self, start: str, end: str) -> dict:
        # Load raw statcast_pitchers for the period
        with RDSConnection().get_connection() as conn:
            df = pd.read_sql(
                "SELECT * FROM statcast_pitchers WHERE game_date BETWEEN %s AND %s",  # noqa: E501
                conn, params=[start, end]
            )

        if df.empty:
            return {"success": True, "rows": 0}

        # Compute game-level aggregates
        result_df = self._aggregate_pitcher_stats(df)

        # Store to RDS
        count = store_dataframe_to_rds(
            result_df,
            "game_level_starting_pitchers",
            if_exists="append"
        )
        return {"success": True, "rows": count}

    def _aggregate_pitcher_stats(self, df: pd.DataFrame) -> pd.DataFrame:
        # Identify starting pitchers: first inning, ≥30 pitches, pitched ≥3 innings
        starters = df.groupby(['game_pk', 'pitcher']).agg({
            'inning': ['min', 'max'],
            'pitch_number': 'count'
        }).reset_index()
        starters.columns = [
            'game_pk', 'pitcher', 'min_inning', 'max_inning', 'total_pitches'
        ]
        mask = (
            (starters['min_inning'] == 1) &
            (starters['total_pitches'] >= 30) &
            (starters['max_inning'] >= 3)
        )
        starter_games = starters[mask][['game_pk', 'pitcher']]

        # Filter to only starter data
        starter_data = df.merge(starter_games, on=['game_pk', 'pitcher'])

        # Flags for metrics
        starter_data['swing_miss'] = starter_data['description'].isin([
            'swinging_strike', 'swinging_strike_blocked'
        ])
        starter_data['first_strike'] = (
            (starter_data['pitch_number'] == 1) & (starter_data['type'] == 'S')
        )

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

        # Aggregate counts
        agg_dict = {
            'game_date': 'first',
            'pitching_team': 'first',
            'opponent_team': 'first',
            'inning': 'nunique',    # innings pitched
            'pitch_number': 'count',# total pitches
            'events': lambda x: x.isin([
                'strikeout', 'strikeout_double_play'
            ]).sum()  # strikeouts
        }
        result = starter_data.groupby(
            ['game_pk', 'pitcher']
        ).agg(agg_dict).reset_index()

        # Rate metrics
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
        result = result.merge(rate_df, on=['game_pk', 'pitcher'], how='left')

        # Rename to match schema
        result = result.rename(columns={
            'pitcher': 'pitcher_id',
            'inning': 'innings_pitched',
            'pitch_number': 'pitches',
            'events': 'strikeouts'
        })

        # Fill NaNs in rates
        result[['swinging_strike_rate', 'first_pitch_strike_rate']] = (
            result[['swinging_strike_rate', 'first_pitch_strike_rate']]
            .fillna(0.0)
        )

        return result