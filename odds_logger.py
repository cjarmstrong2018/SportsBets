import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
from dotenv import load_dotenv
from sportsbooks import SportsBooks
load_dotenv()

DATA_DIR = "mlb_odds"
DATA_DIR_PATH = os.path.dirname(__file__) + "\\" + DATA_DIR


class OddsLogger(object):
    """
    Object to automatically odds for backtesting purposes.
    Given a list of games, formats those whose start_time is within an 
    hour of the current time into a .csv file under the appropriate diretory
    current time and 

    Args:
        sport (str): the sport to log the odds on the sportsbooks. Development to come
    """

    def __init__(self, sport):
        self.__api_key = os.getenv('API_KEY')
        base_url = "https://api.the-odds-api.com"
        odds_endpoint = f"/v3/odds/"
        params = {
            "apiKey": self.__api_key,
            "sport": 'baseball_mlb',
            "region": 'us',
            'mkt': 'h2h',
            'oddsFormat': 'american'
        }
        odds_req = requests.get(base_url + odds_endpoint, params=params)
        print(odds_req.headers['x-requests-remaining'])
        odds = json.loads(odds_req.text)
        odds = odds['data']
        self.games = []
        for game in odds:
            if dt.fromtimestamp(game['commence_time']) < dt.now():  # Ignore live odds
                continue
            row = pd.Series()
            row["ID"] = game['id']
            row['Sport'] = game['sport_nice']
            row['Home'] = game['home_team']
            row['Away'] = [x for x in game['teams'] if x != game['home_team']][0]
            row['Start Time'] = dt.fromtimestamp(game['commence_time'])
            home_first = True if game['teams'][0] == game['home_team'] else False
            odds_by_sb = self.get_all_odds(
                game['sites'], home_first=home_first, draw_possible=False)
            books_quoting = [s['site_key'] for s in game['sites']]
            for book in SportsBooks:
                if book.name in books_quoting:
                    last_update = dt.fromtimestamp(
                        odds_by_sb[f"{book.name}_last_update"])
                    row[f'{book.name}_last_update'] = last_update
                    row[f"{book.name}_home"] = odds_by_sb[f"{book.name}_home"]
                    row[f"{book.name}_away"] = odds_by_sb[f"{book.name}_away"]
                else:
                    row[f'{book.name}_last_update'] = np.nan
                    row[f"{book.name}_home"] = np.nan
                    row[f"{book.name}_away"] = np.nan
            self.games.append(row)
        self.odds_frame = pd.DataFrame(self.games).set_index("ID")
        self.odds_by_month = self.split_months()
        self.merge_with_existing_odds()

    def get_all_odds(self, sites, home_first=True, draw_possible=False):
        '''
        Given the sites value from an Odds-API request
        return a formatted dict of all of the odds for logging
        Inputs:
            sites: list of odds from various dicts of sports betting odds
            draw_possible: (bool) boolean to denote whether the sport has draws in H2H odds
                            (not currently supported) 
        '''
        odds = {}
        for site in sites:
            name = site['site_key']
            last_update = site['last_update']
            line = site['odds']['h2h']
            if home_first:
                home_odds = line[0]
                away_odds = line[1]
            else:
                home_odds = line[1]
                away_odds = line[0]
            odds[name + '_home'] = home_odds
            odds[name + '_away'] = away_odds
            odds[name + '_last_update'] = last_update
            if draw_possible:
                odds[name + '_draw'] = line[-1]
        return odds

    def split_months(self):
        g = self.odds_frame.groupby(pd.Grouper(key="Start Time", freq="M"))
        return [group for _, group in g]

    def merge_with_existing_odds(self):
        """
        Iterates through dataframes in self.odds_by_month and merges current
        data with existing data prioritizing the most current lines
        """
        if not os.path.isdir(DATA_DIR_PATH):
            os.makedirs(DATA_DIR_PATH)
        for m in self.odds_by_month:
            month = m['Start Time'][0].strftime("%B")
            year = m['Start Time'][0].year
            path = DATA_DIR_PATH + "\\" + month + "_" + str(year) + ".csv"
            print(f"Saving {path}")
            if not os.path.exists(path):
                m.to_csv(path)
            else:
                df = pd.read_csv(path, index_col="ID")
                df = pd.concat([df, m], axis=0)
                df = df[~df.index.duplicated(keep='last')]
                df.to_csv(path)


if __name__ == '__main__':
    OddsLogger("baseball_mlb")
