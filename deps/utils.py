import requests
from datetime import datetime
import pandas as pd
from pybaseball import batting_stats_range

def get_date_range(start="2019-03-15", end=datetime.now().strftime("%Y-%m-%d")):
  return [x.strftime("%Y-%m-%d") for x in pd.date_range(start=start, end=end)]

def get_gamelog_range(date_range):
    game_logs = []
    for d in date_range:
        try:
          b = batting_stats_range(d)
          b['date'] = d
          game_logs.append(b)
        except:
          next
    game_logs_df = pd.concat(game_logs, axis=0, ignore_index=True)
    game_logs_df.columns = [c.replace("#", "").replace("2", "_2").replace("3", "_3") for c in game_logs_df.columns]
    return game_logs_df


def mlb_injuries():
  r = requests.get("http://mlb.mlb.com/fantasylookup/json/named.wsfb_news_injury.bam")
  injury_dict = r.json().get("wsfb_news_injury").get("queryResults").get("row")
  injury_list = [pd.DataFrame(p,index=[0]) for p in injury_dict]
  return pd.concat(injury_list, axis=0, ignore_index=True)
