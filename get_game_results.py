import pandas as pd
import numpy as np
from datetime import datetime as dt
import os


class MLBScores(object):
    def __init__(self, year=None) -> None:
        if not os.path.isdir("mlb_results"):
            os.mkdir("mlb_results")
        if year:
            self.year = year
        else:
            self.year = dt.now().year
        self.team_codes = ['ATL', 'ARI', 'BAL', 'BOS', 'CHC', 'CHW', 'CIN', 'CLE', 'COL', 'DET',
                           'KCR', 'HOU', 'LAA', 'LAD', 'MIA', 'MIL', 'MIN', 'NYM', 'NYY', 'OAK',
                           'PHI', 'PIT', 'SDP', 'SEA', 'SFG', 'STL', 'TBR', 'TEX', 'TOR', 'WSN']
        for team in self.team_codes:
            link = f'https://www.baseball-reference.com/teams/{team}/{self.year}-schedule-scores.shtml#team_schedule'
            df = pd.read_html(link, index_col="Gm#")[0]
            df = self.clean_df(df)
            df.to_csv(f"mlb_results\\{team}_{self.year}.csv")

    def clean_df(self, df):
        """
        Cleans initial df from baseball reference

        Args:
            df: DataFrame Object
        """
        df = df.iloc[:, [0, 1, 2, 3, 4, 5, 6, 7]]
        df = df[df.index != 'Gm#']
        df = df.rename(columns={'Unnamed: 2': "before_or_after",
                                'Unnamed: 4': 'H/A'})
        df = df[df['before_or_after'] == 'boxscore']
        df['Home'] = np.where(df['H/A'] == '@', df['Opp'], df['Tm'])
        df['Home Score'] = np.where(df['H/A'] == '@', df['RA'], df['R'])
        df['Away'] = np.where(df['H/A'] == '@', df['Tm'], df['Opp'])
        df['Away Score'] = np.where(df['H/A'] == '@', df['R'], df['RA'])
        df['Home Win'] = np.where(
            df['Home Score'] > df['Away Score'], True, False)
        df['Date'] += " 2022"
        df['Date'] = pd.to_datetime(df['Date'], format="%A, %b %d %Y")
        df = df[['Date', 'Home', 'Home Score', 'Away', 'Away Score', 'Home Win']]
        return df


if __name__ == '__main__':
    MLBScores(2022)
