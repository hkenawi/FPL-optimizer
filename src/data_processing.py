# Data Processing
"""
This script will contain functions for cleaning each individual dataset
"""

import pandas as pd

def cleanFBRefPlayerOffensiveData() -> pd.DataFrame:
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
    df = cleanFBRefPlayerOffensiveData()
