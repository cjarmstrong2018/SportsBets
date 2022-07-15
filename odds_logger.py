import requests
import json
import pandas as pd
import numpy as np
from datetime import datetime as dt
import os
from dotenv import load_dotenv
from sympy import symbols, Eq, solve
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
        odds = json.loads(odds_req.text)
        odds = odds['data']
        self.games = []
        no_arb = True
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

            if self.arb_exists(odds_by_sb):
                # Calculate surebet
                msg_dict = beat_bookies(row['Best Home Odds'], row['Home'], row['Best Book Home'],
                                        row['Best Odds Away'], row['Away'], row['Best Book Away'])
                alert = self.format_alert(msg_dict)
                DiscordAlert(alert)
                no_arb = False

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
        odds['Best Book Home'] = None
        odds['Best Book Away'] = None
        odds['Best Odds Home'] = -10000
        odds['Best Odds Away'] = -10000
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

            odds['Best Odds Home'] = home_odds if home_odds > odds['Best Odds Home'] else odds['Best Odds Home']
            odds['Best Odds Away'] = away_odds if away_odds > odds['Best Odds Away'] else odds['Best Odds Away']
            odds['Best Book Home'] = name if home_odds > odds['Best Odds Home'] else odds['Best Book Home']
            odds['Best Book Away'] = name if away_odds > odds['Best Odds Away'] else odds['Best Book Away']
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

    def arb_exists(self, odds_by_sb):
        sure_bet = 0
        best_odds = [odds_by_sb[x]
                     for x in odds_by_sb.keys() if "Best Odds" in x]
        for odds in best_odds:
            d_odds = decimal_odds(odds)
            sure_bet += 1 / d_odds
        if sure_bet < 1:
            return True
        else:
            return False

    def format_msg(self, msg_dict):
        """
        Formats Message to be passed to DiscordAlert for notification
        Args:
            msg_dict (dict): Dictionary from beat_bookies
        """
        heading = "ALERT: Arb Spotted! ACT FAST" + "\n"
        intro = f"For a total stake of {msg_dict['Total Stake']} place the following bets: \n \n"
        bets = f"{msg_dict['Home Book']}: {msg_dict['Home Team']} ML at {msg_dict['Home Odds']} for {msg_dict['Home Stake']} \n" + \
            f"{msg_dict['Away Book']}: {msg_dict['Away Team']} ML at {msg_dict['Away Odds']} for {msg_dict['Away Stake']} \n"
        profit = f"This will result in a profit of {msg_dict['Home Profit']} or {msg_dict['Away Profit']}"
        return heading + intro + bets + profit


def decimal_odds(odds: int) -> float:
    """
    :param odds: Integer (e.g., -350).
    :return: Float. Odds expressed in Decimal terms.
    """
    if isinstance(odds, float):
        return odds

    elif isinstance(odds, int):
        if odds >= 100:
            return abs(1 + (odds / 100))
        elif odds <= -101:
            return 100 / abs(odds) + 1
        else:
            return float(odds)


def beat_bookies(home_odds, home_team, home_book, away_odds, away_team, away_book, total_stake):
    x, y = symbols('x y')
    eq1 = Eq(x + y - total_stake, 0)  # total_stake = x + y
    eq2 = Eq((away_odds*y) - home_odds*x, 0)  # odds1*x = odds2*y
    stakes = solve((eq1, eq2), (x, y))
    total_investment = stakes[x] + stakes[y]
    profit1 = home_odds*stakes[x] - total_stake
    profit2 = away_odds*stakes[y] - total_stake
    benefit1 = f'{profit1 / total_investment * 100:.2f}%'
    benefit2 = f'{profit2 / total_investment * 100:.2f}%'
    dict_gabmling = {'Home Odds': home_odds, 'Away Odds': away_odds, 'Home Stake': f'${stakes[x]:.0f}', 'Away Stake': f'${stakes[y]:.0f}', 'Home Profit': f'${profit1:.2f}', 'Away Profit': f'${profit2:.2f}',
                     'Benefit1': benefit1, 'Benefit2': benefit2, "Home Book": home_book, "Home Team": home_team, 'Away Book': away_book, 'Away Team': away_team, "Total Stake": total_stake}
    return dict_gabmling


class DiscordAlert(object):
    """
    Class to send a message to my discord bot
    Args:
        msg (str):message to send to discord channel 
    Returns:
        nothing, posts message to channel
    """

    def __init__(self, msg) -> None:
        self.url = "https://discord.com/api/webhooks/997338106258796674/m7M3DKqc12-cNphOw6qJMeWdZzo7wkDjXp1fwZihSlOV2HsQ8NkWBrR2XU25QL9h3Z8n"
        data = {'content': msg}
        r = requests.post(url=self.url, json=data)


if __name__ == '__main__':
    OddsLogger("baseball_mlb")
