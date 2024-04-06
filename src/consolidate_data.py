# Data consolidation

import pandas as pd

def consolidateFBRefPlayerOffensiveData() -> pd.DataFrame:
    """
    Consolidates offensive player data for seasonal historical averages
    :return: A stacked dataframe with data for all years
    """
    df = pd.DataFrame()
    seasons = ['2018_19', '2019_20', '2020_21', '2021_22', '2022_23']

    for season in seasons:
        temp_df = pd.read_csv(filepath_or_buffer=f'.data/raw data/{season}_player_offensive_stats.csv',
                              header=1)
        temp_df['season'] = season
        temp_df = temp_df[temp_df['Rk'].apply(lambda x: str(x).isdigit())]
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
