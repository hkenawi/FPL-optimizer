# Scrape player data from FBRef

"""
Scrapes player match-log data from FBRef
Functions:
    Primary:
        scrapeMatchLogTable
    Secondary:
        getPlayerLinks
"""

import bs4
import time
import requests
import pandas as pd

def getPlayerLinks(url: str) -> list:
    """
    Retrieves distinct href URLs for each player match-log page

    :param url: The URL to the main page for the season statistics
    :return: A list of unique URLs for each player's match-log page
    """

    r = requests.get(url)
    soup = bs4.BeautifulSoup(r.content, "html.parser")

    table_id = """<div class="table_container" id="div_stats_standard">"""
    table = (soup.find(attrs={"data-label": "Player Standard Stats"})
             .find_next(string=lambda tag:
             isinstance(tag, bs4.element.Comment) and table_id in tag))
    links = bs4.BeautifulSoup(table).find_all('a')
    player_links = []
    keyword = '/en/players/'
    for link in links:
        if keyword in str(link):
            player_links.append(str(link))
    player_links = [link for link in player_links if 'Match-Logs' in link]
    player_links = [link.replace('<a href="', '') for link in player_links]
    player_links = [link.replace('">Matches</a>', '') for link in player_links]

    return player_links


def scrapeMatchLogTable() -> None:
    """
    Scrape the match-log data for each player for seasons 2018/19-2022/23
    :return: Stacked dataframe with all seasons data
    """
    main_page_links = ['https://fbref.com/en/comps/9/2022-2023/stats/2022-2023-Premier-League-Stats',
                       'https://fbref.com/en/comps/9/2021-2022/stats/2021-2022-Premier-League-Stats'
                       'https://fbref.com/en/comps/9/2020-2021/stats/2020-2021-Premier-League-Stats',
                       'https://fbref.com/en/comps/9/2019-2020/stats/2019-2020-Premier-League-Stats',
                       'https://fbref.com/en/comps/9/2018-2019/stats/2018-2019-Premier-League-Stats']

    for main_link in main_page_links:
        links = getPlayerLinks(url=main_link)
        print(f'Got player links for {main_link[29:38]}')
        df = pd.DataFrame()
        url = 'https://fbref.com'
        for link in links:
            for i in range(3):
                try:
                    full_url = url + link
                    temp_df = pd.read_html(full_url)[0]
                    temp_df['Name'] = link
                    df = pd.concat([df, temp_df])
                    print(f'{link}: complete')
                except Exception:
                    print(f'Retry: {i}')
                    time.sleep(1)
                else:
                    time.sleep(1)
                    break
        df.to_csv(f'./data/raw data/{main_link[29:38].replace("-", "_")}_player_match_log_data.csv')


if __name__ == "__main__":
    scrapeMatchLogTable()
