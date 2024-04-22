# Team-level Data Processing
"""
This script will contain functions for cleaning team-level data
Functions:
    Primary:
        mergeTeamAdvNormGkData
        cleanTeamDefensiveData
    Secondary:
        cleanTeamGkData
        cleanTeamAdvGkData
"""

import pandas as pd


def cleanTeamGkData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of team goalkeeping data
    :return: Cleaned team goalkeeping data
    """
    df = pd.read_csv('../data/consolidated_team_goalkeeping_stats.csv')
    df.rename(columns={'Save%.1': 'PKsv%'},
              inplace=True)
    df['PKsv%'].fillna(value=0,
                       inplace=True)
    df['Squad'].replace('Nott\'ham Forest', 'Nottingham Forest',
                        inplace=True)
    df['SoTA90'] = df['SoTA']/df['90s']
    df['Saves90'] = df['Saves'] / df['90s']

    columns_to_keep = ['Squad', 'season', 'GA90', 'SoTA90', 'Saves90', 'Save%', 'CS%', 'PKsv%']

    return df[columns_to_keep]


def cleanTeamAdvancedGkData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of team advanced goalkeeping data
    :return: Cleaned team advanced goalkeeping data
    """
    df = pd.read_csv('../data/consolidated_team_advanced_goalkeeping_stats.csv')
    df.rename(columns={'/90': 'PSxG/90',
                       'Launch%': 'Pass_Launch%',
                       'AvgLen': 'PassAvgLen',
                       'Launch%.1': 'Goalkick_Launch%',
                       'AvgLen.1': 'GoalkickAvgLen',
                       'Stp%': 'Cross_Stp%'},
              inplace=True)
    df['Squad'].replace('Nott\'ham Forest', 'Nottingham Forest',
                        inplace=True)

    columns_to_keep = ['Squad', 'season', 'PSxG/SoT', 'PSxG/90', 'PSxG+/-', 'Cmp%', 'Pass_Launch%',
                       'PassAvgLen', 'Goalkick_Launch%', 'GoalkickAvgLen', 'Cross_Stp%', '#OPA/90', 'AvgDist']

    return df[columns_to_keep]


def mergeTeamAdvNormGkData() -> pd.DataFrame:
    """
    Merges normal and advanced team goalkeeping data by team and season
    :return: Merged team goalkeeping data with regular and advanced statistics
    """
    normal_data = cleanTeamGkData()
    advanced_data = cleanTeamAdvancedGkData()

    return normal_data.merge(advanced_data,
                             how='left',
                             on=['Squad', 'season'],
                             copy=False)


def cleanTeamDefensiveData() -> pd.DataFrame:
    """
    Conducts necessary cleaning of team defensive actions data
    :return: Cleaned team defensive actions data
    """
    df = pd.read_csv('../data/consolidated_team_defensive_stats.csv')
    df.rename(columns={'Def 3rd': 'tkl_def_3rd',
                       'Mid 3rd': 'tkl_mid_3rd',
                       'Att 3rd': 'tkl_att_3rd',
                       'Sh': 'shots_blocked',
                       'Pass': 'passes_blocked'},
              inplace=True)
    df['Squad'].replace('Nott\'ham Forest', 'Nottingham Forest',
                        inplace=True)

    columns_to_keep = ['Squad', 'season', 'Tkl%', 'tkl_def_3rd', 'tkl_mid_3rd', 'tkl_att_3rd',
                       'shots_blocked', 'passes_blocked', 'Int', 'Tkl+Int', 'Clr', 'Err']
    df = df[columns_to_keep]

    df.iloc[:, 3:] = df.iloc[:, 3:].div(38,
                                        axis=0)

    return df


if __name__ == "__main__":
    df1 = mergeTeamAdvNormGkData()
    df2 = cleanTeamDefensiveData()
