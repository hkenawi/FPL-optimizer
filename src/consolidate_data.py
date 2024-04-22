# Data consolidation
"""
This script will contain raw data consolidation functions
Functions:
    consolidateFBRefPlayerOffensiveData
    consolidateFBRefTeamDefensiveData
    consolidateFBRefTeamGoalkeepingData
    consolidateFBRefPlayerMatchData
"""

import pandas as pd

def consolidateFBRefPlayerOffensiveData() -> pd.DataFrame:
    """
    Consolidates offensive player data for seasonal historical averages
    :return: A stacked dataframe with data for all years
    """
    df = pd.DataFrame()
    seasons = ['2018_19', '2019_20', '2020_21', '2021_22', '2022_23']

    for season in seasons:
        temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_player_offensive_stats.csv',
                              header=1)
        temp_df['season'] = season
        temp_df = temp_df[temp_df['Rk'].apply(lambda x: str(x).isdigit())]
        df = pd.concat([df, temp_df])
        df.reset_index(drop=True,
                       inplace=True)

    return df


def consolidateFBRefTeamDefensiveData() -> pd.DataFrame:
    """
    Consolidates defensive team data for seasonal historical averages
    :return: A stacked dataframe with data for all years
    """
    df = pd.DataFrame()
    seasons = ['2018_19', '2019_20', '2020_21', '2021_22', '2022_23']

    for season in seasons:
        if season == '2018_19':
            temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_team_defensive_stats.csv')
        else:
            temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_team_defensive_stats.csv',
                                  header=1)
        temp_df['season'] = season
        df = pd.concat([df, temp_df])
        df.reset_index(drop=True,
                       inplace=True)

    return df


def consolidateFBRefTeamGoalkeepingData(advanced: bool = False) -> pd.DataFrame:
    """
    Consolidates defensive team data for seasonal historical averages

    :param advanced: States whether to consolidate advanced goalkeeping data.
                     Options are True or False.
                     True retrieves advanced goalkeeper data
                     False retrieves standard goalkeeper data
    :return: A stacked dataframe with data for all years
    """
    df = pd.DataFrame()
    seasons = ['2018_19', '2019_20', '2020_21', '2021_22', '2022_23']

    for season in seasons:
        if not advanced:
            if season == '2022_23':
                temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_team_goalkeeping_stats.csv')
            else:
                temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_team_goalkeeping_stats.csv',
                                      header=1)
        else:
            temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_team_adv_goalkeeping_stats.csv',
                                  header=1)
        temp_df['season'] = season
        df = pd.concat([df, temp_df])
        df.reset_index(drop=True,
                       inplace=True)

    return df


def consolidateFBRefPlayerMatchData() -> pd.DataFrame:
    """
    Consolidates player match-log data for seasons 2018/19-2022/23
    :return: A stacked dataframe with data for all years
    """
    df = pd.DataFrame()
    seasons = ['2018_2019', '2019_2020', '2020_2021', '2021_2022', '2022_2023']

    for season in seasons:
        temp_df = pd.read_csv(filepath_or_buffer=f'./data/raw data/{season}_player_match_log_data.csv',
                              header=1)
        temp_df['season'] = season
        df = pd.concat([df, temp_df])
        df.reset_index(drop=True,
                       inplace=True)

    return df


if __name__ == "__main__":
    stacked_offensive_player_df = consolidateFBRefPlayerOffensiveData()
    stacked_offensive_player_df.to_csv('./data/consolidated_player_offensive_stats.csv')

    stacked_match_log_df = consolidateFBRefPlayerMatchData()
    stacked_match_log_df.to_csv('./data/consolidated_player_match_log_data.csv')

    stacked_defensive_team_df = consolidateFBRefTeamDefensiveData()
    stacked_defensive_team_df.to_csv('./data/consolidated_team_defensive_stats.csv')

    stacked_gk_team_df = consolidateFBRefTeamGoalkeepingData(advanced=False)
    stacked_gk_team_df.to_csv('./data/consolidated_team_goalkeeping_stats.csv')

    stacked_adv_gk_team_df = consolidateFBRefTeamGoalkeepingData(advanced=True)
    stacked_adv_gk_team_df.to_csv('./data/consolidated_team_advanced_goalkeeping_stats.csv')
