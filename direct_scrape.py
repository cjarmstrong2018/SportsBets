import pandas as pd
import numpy as np
import json
from selenium import webdriver
from selenium.common.exceptions import TimeoutException
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from datetime import datetime
from datetime import timedelta
import requests
from bs4 import BeautifulSoup
from requests.auth import HTTPBasicAuth
from selenium.webdriver.common.keys import Keys
import urllib.request
import time
import warnings
import re
import os
import http.client
from flatten_json import flatten


warnings.filterwarnings("ignore")

DATA_DIR_PATH = os.path.dirname(__file__) + "\\"
SPORTS = {
    "MLB": 'baseball_mlb',
    'NBA': 'basketball_nba',
    "NFL": 'football_nfl',
    "NCAAB": 'basketball_ncaa'
}
DATA_DIRS = {"MLB": "mlb_odds",
             "NBA": 'nba_odds',
             "NFL": 'nfl_odds',
             "NCAAB": "ncaab_odds"
             }


class Scraper(object):
    def __init__(self, sportsbook, league) -> None:
        self.sportsbook = sportsbook
        self.league = league
        self.data = None
        self.data_dir_path = DATA_DIR_PATH + self.league + "\\" + self.sportsbook
        self.odds_by_month = None

    def save_data(self):
        """
        Checks if appropriate folder exists and opens, merges and saves the data from scraper
        This must be used after a super class gathers data on top
        """
        self.split_months()
        self.merge_with_existing_odds()

    def split_months(self):
        g = self.data.groupby(pd.Grouper(key="date", freq="M"))
        self.odds_by_month = [group for _, group in g]

    def merge_with_existing_odds(self):
        """
        Iterates through dataframes in self.odds_by_month and merges current
        data with existing data prioritizing the most current lines
        """
        if not os.path.isdir(self.data_dir_path):
            os.makedirs(self.data_dir_path)
        for m in self.odds_by_month:
            month = m['date'][0].strftime("%B")
            year = m['date'][0].year
            path = self.data_dir_path + "\\" + month + "_" + str(year) + ".csv"
            print(f"Saving {path}")
            if not os.path.exists(path):
                m.to_csv(path)
            else:
                df = pd.read_csv(path)
                df = pd.concat([df, m], axis=0)
                df = df[~df.index.duplicated(keep='last')]
                df.to_csv(path)


class BarstoolSportsbook(Scraper):
    def __init__(self, league) -> None:
        super().__init__("Barstool", league)
        if league == "NBA":
            self.sport = "basketball"
            self.url = "https://www.barstoolsportsbook.com/sports/basketball/nba?category=upcoming"
        elif league == "MLB":
            self.sport = 'baseball'
            self.url = "https://www.barstoolsportsbook.com/sports/baseball/mlb?category=upcoming"
        elif league == "NFL":
            self.sport = 'football'
            self.url = "https://www.barstoolsportsbook.com/sports/american_football/nfl?category=upcoming"
        elif league == "NCAAF":
            self.sport = "football"
            self.url = "https://www.barstoolsportsbook.com/sports/american_football/ncaaf?category=upcoming&subcategory=All"

        op = webdriver.ChromeOptions()
        # op.add_argument('headless')

        web = webdriver.Chrome(
            "C:\\Users\\chris\\OneDrive\\Projects\\SportsBets\\chromedriver", options=op)
        web.get(self.url)
        timeout = 10
        try:
            element_present = EC.presence_of_element_located(
                (By.CLASS_NAME, 'basic-event-row'))
            WebDriverWait(web, timeout).until(element_present)
            print("Page Loaded!")
        except TimeoutException:
            print("Timed out waiting for page to load")

        soup = BeautifulSoup(web.page_source)
        web.close()
        games = soup.find_all('div', class_="basic-event-row")
        data = []
        for game in games:
            # get dates
            date = game.find(
                "p", class_="start-display strongbody2").text.strip()
            if "Today" in date:
                today = datetime.now().strftime("%a, %b %d,").strip()
                date = date.split(',')[-1]
                date = today + date
            date = datetime.strptime(date, "%a, %b %d, %I:%M %p")
            date = date.replace(year=datetime.now().year)
            # get teams
            participants = game.find("div", class_="row participant-row")
            participants = participants.find(
                "div", class_=re.compile("^participant"))
            participants = participants.find_all('p')

            away_team = participants[0].text.strip(" 0123456789")
            if league == "MLB":
                home_team = participants[2].text.strip(" 0123456789")
            else:
                home_team = participants[1].text.strip(" 0123456789")

            # spread
            spread_tag = game.find('div', class_="bet-offer col col-4")
            spread_lines = spread_tag.find_all("div", class_='desc')
            if not spread_lines:
                away_line = home_line = np.nan
                away_spread_odds = home_spread_odds = np.nan
            else:
                away_line, home_line = [float(x.text) for x in spread_lines]
                spread_odds_tags = spread_tag.find_all("div", class_='odds')
                away_spread_odds, home_spread_odds = [
                    int(x.text) for x in spread_odds_tags]

            # moneyline odds
            moneyline_tag = game.find('div', class_="col col-4 bet-offer")
            moneyline_odds = moneyline_tag.find_all("div", class_='odds')
            if not moneyline_odds:
                away_odds = np.nan
                home_odds = np.nan
            else:
                away_odds, home_odds = [int(x.text) for x in moneyline_odds]

            # O/U odds
            ou_tag = game.find(
                'div', class_="bet-offer col col-4").find_next_sibling('div', class_='bet-offer col col-4')
            ou_lines = ou_tag.find_all("div", class_='desc')
            if not ou_lines:
                over_line = np.nan
                under_line = np.nan
                over_odds = np.nan
                under_odds = np.nan
            else:
                over_line, under_line = [
                    float(x.text.split()[-1]) for x in ou_lines]
                ou_odds_tags = ou_tag.find_all("div", class_='odds')
                over_odds, under_odds = [int(x.text) for x in ou_odds_tags]

            # create data dict
            entry = {
                "date": date,
                'home': home_team,
                'away': away_team,
                "home moneyline": home_odds,
                'away odds': away_odds,
                "home spread line": home_line,
                "home spread odds": home_spread_odds,
                "away spread line": away_line,
                "away spread odds": away_spread_odds,
                "over line": over_line,
                "over odds": over_odds,
                "under line": under_line,
                "under odds": under_odds
            }

            data.append(entry)
        self.data = pd.DataFrame(data)
        self.save_data()


class BetMGM(Scraper):
    def __init__(self, league) -> None:
        super().__init__("BetMGM", league)
        if league == "NBA":
            self.sport = "basketball"
            self.url = "https://sports.il.betmgm.com/en/sports/basketball-7/betting/usa-9/nba-6004"
        elif league == "MLB":
            self.sport = 'baseball'
            self.url = "https://sports.il.betmgm.com/en/sports/baseball-23/betting/usa-9/mlb-75"
        elif league == "NFL":
            self.sport = 'football'
            self.url = "https://sports.il.betmgm.com/en/sports/football-11/betting/usa-9/nfl-35"
        elif league == "NCAAF":
            self.sport = "football"
            self.url = "https://sports.il.betmgm.com/en/sports/football-11/betting/usa-9/college-football-211"

        op = webdriver.ChromeOptions()
        op.add_argument(
            "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/105.0.0.0 Safari/537.36")
        op.add_argument('headless')
        op.add_argument("--disable-web-security")
        op.add_argument("--disable-blink-features=AutomationControlled")

        web = webdriver.Chrome(
            "C:\\Users\\chris\\OneDrive\\Projects\\SportsBets\\chromedriver", options=op)
        web.get(self.url)
        timeout = 10
        try:
            element_present = EC.presence_of_element_located(
                (By.CLASS_NAME, "participants-pair-game"))
            WebDriverWait(web, timeout).until(element_present)
            print("Page Loaded!")
        except TimeoutException:
            print("Timed out waiting for page to load")

        soup = BeautifulSoup(web.page_source)
        web.close()
        games = soup.find_all('ms-six-pack-event')
        data = []
        for game in games:
            is_live = game.find('i', class_=re.compile("^live"))
            if is_live:
                print("LIVE")
                continue
            # get dates
            start = game.find("ms-event-timer", class_='grid-event-timer')
            if start is None:
                continue
            else:
                start = start.text
            if "Starting" in start:
                mins_to_start = int(start.split(' ')[-2])
                date = datetime.now() + timedelta(minutes=mins_to_start)
            elif "Today" in start:
                start = start.split()[-2:]
                start = " ".join(start)
                start = datetime.strptime(start, "%H:%M %p")
                date = datetime.now()
                date = date.replace(hour=start.hour, minute=start.minute)
            elif "Tomorrow" in start:
                start = start.split()[-2:]
                start = " ".join(start)
                start = datetime.strptime(start, "%I:%M %p")
                date = datetime.now() + timedelta(days=1)
                date = date.replace(hour=start.hour, minute=start.minute)
            else:
                start = re.sub("•", "", start)
                date = datetime.strptime(start, "%m/%d/%y  %I:%M %p")
            date = date.replace(second=0, microsecond=0)
        #     # get teams
            participants = game.find_all("div", class_="participant")
            # addresses any special matches or lines listed
            if not participants:
                break
            away_team, home_team = [x.text.strip() for x in participants]
            lines = game.find_all("ms-option-group")
            spread_tag, ou_tag, moneyline_tag = lines
            # spread
            spread_lines = spread_tag.find_all(
                "div", class_=r'option-attribute')
            if not spread_lines:
                away_line = home_line = np.nan
                away_spread_odds = home_spread_odds = np.nan
            else:
                away_line, home_line = [float(x.text) for x in spread_lines]

                spread_odds_tags = spread_tag.find_all(
                    "div", class_='option option-value')
                away_spread_odds, home_spread_odds = [
                    int(x.text) for x in spread_odds_tags]

            # O/U odds

            ou_lines = ou_tag.find_all(
                "div", class_=re.compile('^option-attribute'))
            if not ou_lines:
                over_line = np.nan
                under_line = np.nan
                over_odds = np.nan
                under_odds = np.nan
            else:
                over_line, under_line = [
                    float(x.text.split()[-1]) for x in ou_lines]
                ou_odds_tags = ou_tag.find_all(
                    "div", class_='option option-value')
                over_odds, under_odds = [int(x.text) for x in ou_odds_tags]
            # moneyline odds
            moneyline_odds = moneyline_tag.find_all(
                "div", class_="option option-value")
            if not moneyline_odds:
                away_odds = np.nan
                home_odds = np.nan
            else:
                away_odds, home_odds = [int(x.text) for x in moneyline_odds]
            # create data dict
            entry = {
                "date": date,
                'home': home_team,
                'away': away_team,
                "home moneyline": home_odds,
                'away odds': away_odds,
                "home spread line": home_line,
                "home spread odds": home_spread_odds,
                "away spread line": away_line,
                "away spread odds": away_spread_odds,
                "over line": over_line,
                "over odds": over_odds,
                "under line": under_line,
                "under odds": under_odds
            }

            data.append(entry)
        self.data = pd.DataFrame(data)
        print(self.data.columns)
        # self.save_data()


class BetRivers(Scraper):

    def __init__(self, league) -> None:
        super().__init__("BetRivers", league)
        self.group_ids = {
            'NFL': 1000093656,
            "MLB": 1000093616,
            "NBA": 1000093652,
            "NCAAF": 1000093655
        }

        self.group_id = self.group_ids[league]
        conn = http.client.HTTPSConnection("il.betrivers.com")
        conn.request(
            "GET", f"/api/service/sportsbook/offering/listview/events?pageNr=1&cageCode=847&groupId={self.group_id}&=&type=prematch")

        res = conn.getresponse()
        data = res.read()
        data.decode("utf-8")
        data = json.loads(data)

        # Get odds
        odds = pd.json_normalize(data['items'], record_path=['betOffers', 'outcomes'], meta=[
                                 'id', ['betOffers', "betDescription"]], errors='ignore', meta_prefix='Meta.')
        odds = odds[['type', 'oddsAmerican', 'line',
                     'Meta.id', 'Meta.betOffers.betDescription']]
        odds = odds.rename(columns={'Meta.betOffers.betDescription': 'Bet Type',
                                    'Meta.id': "Game ID",
                                    'oddsAmerican': "American Odds"})
        odds = odds.set_index(['Game ID', "Bet Type", 'type'])
        odds = odds.unstack(level=[1, 2])
        odds.columns = ['away spread odds', "home spread odds", "away moneyline", "home moneyline", "tp over odds", "tp under odds",
                        "away spread line", "home spread line", "ML Line Away", "ML Line Home", "tp over line", "tp under line"]
        odds = odds.drop(columns=["ML Line Away", "ML Line Home"])

        teams = pd.json_normalize(data['items'], 'participants', [
                                  'id'], errors="ignore", record_prefix='T1').set_index(['id', 'T1home'])
        teams = teams.unstack(level=1)

        teams = teams.droplevel(0, axis=1)
        teams.columns = ['away', 'home', "Away ID", "Home ID"]
        teams = teams[['away', 'home']]
        teams.index.name = "Game ID"

        games = pd.json_normalize(data, 'items', errors="ignore")
        games = games[['id', 'start']]
        games = games.rename(columns={'id': "Game ID"})
        games = games.set_index('Game ID')

        df = pd.concat([games, teams, odds], axis=1)
        df['start'] = pd.to_datetime(df['start'], utc=True)
        df = df.reset_index().set_index('start')
        df.index = df.index.tz_convert('US/Central')
        df.index = df.index.tz_localize(None)
        df.index.name = 'date'
        self.data = df
        # self.save_data()
        print(self.data)


class DraftKings(Scraper):

    def __init__(self, league) -> None:
        super().__init__("DraftKings", league)
        self.group_ids = {
            'NFL': 88808,
            "MLB": 84240,
            "NBA": 42648,
            "NCAAF": 87637,
            "NHL": 42133
        }
        self.group_id = self.group_ids[league]
        conn = http.client.HTTPSConnection("sportsbook-us-nh.draftkings.com")

        conn.request(
            "GET", f"//sites/US-NH-SB/api/v5/eventgroups/{self.group_id}?format=json")

        res = conn.getresponse()
        data = res.read()

        data.decode("utf-8")
        data = json.loads(data)

        odds_data = data['eventGroup']["offerCategories"][0]["offerSubcategoryDescriptors"][0]["offerSubcategory"]['offers']
        games = data['eventGroup']['events']
        # pd.json_normalize(data)

        games = pd.json_normalize(games)
        games = games[games['eventStatus.state'] == "NOT_STARTED"]
        games = games[['eventId', 'startDate', "teamName1", "teamName2"]]
        games = games.rename(columns={"eventId": "Game ID",
                                      "teamName1": "Away",
                                      "teamName2": "Home"})
        games = games.set_index("Game ID")

        all_odds = []
        for entry in odds_data:
            odds = pd.json_normalize(
                entry, 'outcomes', ['eventId', 'label'], 'game-')
            odds = odds[['label', 'oddsAmerican',
                         "line", 'game-eventId', 'game-label']]
            odds = odds.rename(columns={'game-label': 'Bet Type',
                                        'game-eventId': "Game ID",
                                        'oddsAmerican': "American Odds"})
            odds['label'] = np.where(
                odds['label'] == odds['label'][0], "AWAY", odds['label'])
            odds['label'] = np.where(
                odds['label'] == odds['label'][1], "HOME", odds['label'])
            odds['Bet Type'] = odds['Bet Type'].replace(
                "Total", "Total Points")
            odds['Bet Type'] = odds['Bet Type'].replace(
                "Spread", "Point Spread")

            odds = odds.set_index(['Game ID', "Bet Type", 'label'])
            all_odds.append(odds)

        odds = pd.concat(all_odds)
        odds = odds.unstack(level=[1, 2])
        odds.columns = ['away spread odds', "home spread odds", "tp over odds", "tp under odds", "away moneyline", "home moneyline",
                        "away spread line", "home spread line", "tp over line", "tp under line", "ML Line Away", "ML Line Home"]
        odds = odds.drop(columns=["ML Line Away", "ML Line Home"])

        df = games.merge(odds, how='outer', left_index=True, right_index=True)
        df['startDate'] = pd.to_datetime(df['startDate'], utc=True)
        df = df.reset_index().set_index('startDate')
        df.index = df.index.tz_convert('US/Central')
        df.index = df.index.tz_localize(None)
        df.index.name = 'date'
        self.data = df
        print(self.data.sort_index())


if __name__ == '__main__':
    # BarstoolSportsbook("MLB")
    # BetMGM("NFL")
    # print("BetRivers")
    BetRivers("NBA")
    print("DraftKings")
    # DraftKings("NCAAF")
