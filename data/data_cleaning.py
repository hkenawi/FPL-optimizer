import os
import pandas as pd


def consolidateFPLPlayerOffensiveData(directory):
    dfs = []
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            file_path = os.path.join(directory, filename)
            df = pd.read_csv(file_path, low_memory=False, encoding='latin1')
            df['source_file'] = filename
            dfs.append(df)

    stacked_df = pd.concat(dfs, ignore_index=True)
    if 'kickoff_time' in stacked_df.columns:
        stacked_df['kickoff_time'] = pd.to_datetime(stacked_df['kickoff_time'])
    if 'kickoff_time' in stacked_df.columns:
        stacked_df.sort_values(by='kickoff_time', inplace=True)
        stacked_df = stacked_df[stacked_df['kickoff_time'] >= '2018-08-10']

    return stacked_df
if __name__ == "__main__":
    desktop_path = os.path.join(os.path.expanduser("~"), "Desktop")
    project_directory = "/Users/ismailelshafei/PycharmProjects/pythonProject3"
    stacked_and_sorted_df = consolidateFPLPlayerOffensiveData(project_directory)
    output_file = os.path.join(desktop_path, "stacked_data.csv.gz")
    stacked_and_sorted_df.to_csv(output_file, index=False, compression='gzip')


