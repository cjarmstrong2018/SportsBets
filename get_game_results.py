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
            df = df.iloc[:, [0, 2, 3, 4, 5, 6, 7]]
            df = df[df.index != 'Gm#']
            df.to_csv(f"mlb_results\\{team}_{self.year}.csv")


if __name__ == '__main__':
    MLBScores(2022)
