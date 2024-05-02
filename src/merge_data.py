# Merge data
"""
Merge team-level data with player level data
"""

import pandas as pd

import src.team_data_processing as tdp
import src.player_data_processing as pdp

def main() -> pd.DataFrame:
    team_df = tdp.mergeTeamData()
    team_df['season'] = team_df['season'].str.replace('_', '_20')
    player_df = pdp.mergePlayerData()

    return player_df.merge(team_df,
                           how='left',
                           left_on=['Opponent', 'season'],
                           right_on=['Squad', 'season'],
                           copy=False).drop(columns='Squad')

if __name__ == "__main__":
    df = main()