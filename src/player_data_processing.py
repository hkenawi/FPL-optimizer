# Player-level Data Processing
"""
This script will contain functions for cleaning player-level data
Functions:
    Primary:
        mergePlayerData
    Secondary:
        cleanPlayerMatchLogData
        cleanFBRefPlayerOffensiveData
    Tertiary:
        getWeightedAverage
"""

import numpy as np
import pandas as pd

from unidecode import unidecode


def cleanPlayerMatchLogData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of player match-log data
    :return: Cleaned player match-log data
    """
    df = pd.read_csv('../data/consolidated_player_match_log_data.csv')

    df.dropna(subset=['Date'])
    df = df[df['Comp'] == 'Premier League']
    df = df[df['Min'] != 'On matchday squad, but did not play']

    df.rename(columns={'Unnamed: 38': 'Player'},
              inplace=True)
    df['Player'] = df['Player'].str.split('/').apply(lambda x: x[-1]).str.split('-').apply(lambda x: x[:-2]).str.join(' ')
    df['Player'] = df['Player'].str.replace('-', '').str.replace(' ', '').str.upper().str.strip()
    df['Player'] = df['Player'].apply(unidecode)

    conditions = [df['Venue']=='Home', df['Venue']=='Away']
    choices = (1, 0)
    df['is_home'] = np.select(conditions, choices)

    columns_to_keep = ['Date', 'Opponent', 'Min', 'xG', 'npxG',
                       'xAG', 'Player', 'season', 'is_home']

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

    df['season'] = df['season'].str.replace('_', '_20')
    df['Player'] = df['Player'].str.replace('-', '').str.replace(' ', '').str.upper().str.strip()
    df['Player'] = df['Player'].apply(unidecode)
    df = df[df['90s'] != 0].reset_index(drop=True)

    columns_to_keep = ['Player', 'season', 'SoT%', 'Sh/90',
                       'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'npxG/Sh',
                       'np:G-xG', 'is_pk_taker', '90s']
    df = df[columns_to_keep]

    df = df.groupby(['Player', 'season'],
                    as_index=False).apply(getWeightedAverage,
                                          ['SoT%', 'Sh/90',
                                           'SoT/90', 'G/Sh', 'G/SoT', 'Dist', 'npxG/Sh',
                                           'np:G-xG', 'is_pk_taker'],
                                          '90s')
    return df


def mergePlayerData() -> pd.DataFrame:
    match_data = cleanPlayerMatchLogData()
    offensive_data = cleanFBRefPlayerOffensiveData()

    merged_data = match_data.merge(offensive_data,
                                   how='left',
                                   on=['season', 'Player'],
                                   copy=False)

    return merged_data


def getWeightedAverage(df: pd.DataFrame,
                       columns: list,
                       weight: str) -> pd.Series:
    """
    Calculates weighted average of provided dataframe columns
    :param df: Input dataframe with value columns and weight column
    :param columns: The columns to be averaged
    :param weight: The weight column

    :return: A pandas series with the averaged columns
    """
    return pd.Series(np.average(df[columns],
                                weights=df[weight],
                                axis=0),
                     columns)


if __name__ == "__main__":
    df1 = cleanFBRefPlayerOffensiveData()
    df2 = cleanPlayerMatchLogData()

    df3 = mergePlayerData()