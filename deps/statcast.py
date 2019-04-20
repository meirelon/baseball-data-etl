import os
from datetime import datetime, timedelta
import requests
import io
import pandas as pd

def get_statcast_data():
    start_dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    end_dt = datetime.now().strftime("%Y-%m-%d")
    url = "https://baseballsavant.mlb.com/statcast_search/csv?all=true&hfPT=&hfAB=&hfBBT=&hfPR=&hfZ=&stadium=&hfBBL=&hfNewZones=&hfGT=R%7CPO%7CS%7C=&hfSea=&hfSit=&player_type=pitcher&hfOuts=&opponent=&pitcher_throws=&batter_stands=&hfSA=&game_date_gt={}&game_date_lt={}&team=&position=&hfRO=&home_road=&hfFlag=&metric_1=&hfInn=&min_pitches=0&min_results=0&group_by=name&sort_col=pitches&player_event_sort=h_launch_speed&sort_order=desc&min_abs=0&type=details&".format(start_dt, end_dt)
    s=requests.get(url, timeout=None).content
    return pd.read_csv(io.StringIO(s.decode('utf-8')))
