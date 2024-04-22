# Player-level Data Processing
"""
This script will contain functions for cleaning player-level data
Functions:
    cleanPlayerMatchLogData
    cleanFBRefPlayerOffensiveData
"""

import numpy as np
import pandas as pd


def cleanPlayerMatchLogData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of player match-log data
    :return: Cleaned player match-log data
    """
    df = pd.read_csv('../data/consolidated_player_match_log_data.csv')

    df.dropna(subset=['Date'])
    df = df[df['Comp'] == 'Premier League']
    df = df[df['Min'] != 'On matchday squad, but did not play']

    df.rename(columns={'Unnamed: 38': 'Name'},
              inplace=True)
    df['Name'] = df['Name'].str.split('/').apply(lambda x: x[-1]).str.split('-').apply(lambda x: x[:-2]).str.join(' ')

    conditions = [df['Venue']=='Home', df['Venue']=='Away']
    choices = (1, 0)
    df['is_home'] = np.select(conditions, choices)

    columns_to_keep = ['Date', 'Opponent', 'Min', 'xG', 'npxG',
                       'xAG', 'Name', 'season', 'is_home']

    return df[columns_to_keep]


def cleanFBRefPlayerOffensiveData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of player offensive data
    :return: Cleaned player offensive data
    """
    df = pd.read_csv('../data/consolidated_player_offensive_stats.csv')

    # Create penalty-taker variable
    team_total_pens = pd.DataFrame(df.groupby(['Squad', 'season'])['PKatt'].sum())
    df = df.merge(team_total_pens,
                  how='left',
                  on=['Squad', 'season'],
                  copy=False)
    df.rename(columns={'PKatt_y': 'team_PKatt',
                       'PKatt_x': 'PKatt'},
              inplace=True)
    df['is_pk_taker'] = df['PKatt']/df['team_PKatt'] >= 0.5

    columns_to_keep = ['Player', 'Squad', 'season', 'SoT%', 'Sh/90',
                       'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'npxG/Sh',
                       'np:G-xG', 'is_pk_taker']
    return df[columns_to_keep]


if __name__ == "__main__":
    df1 = cleanFBRefPlayerOffensiveData()
    df2 = cleanPlayerMatchLogData()
