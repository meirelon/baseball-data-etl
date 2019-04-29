from datetime import datetime, timedelta
import pandas as pd
from pybaseball import statcast

def get_statcast_data(start_dt=None, end_dt=None):
    if start_dt is None:
        start_dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    if end_dt is None:
        end_dt = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

    df = statcast(start_dt=start_dt, end_dt=end_dt)

    #clean up the df
    df = df.drop(["index"], axis=1)
    df.columns = [c.replace(".", "_" ) for c in df.columns]
    return df

if __name__ == '__main__':
    get_statcast_data()
