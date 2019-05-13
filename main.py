import os
from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq

import deps.utils as utils
from deps.statcast import get_statcast_data
from deps.seatgeek import seatgeek
from deps.weather import weather

def mlb_daily_etl(request):
    # DEFINE ENVIRONMENT VARIABLES
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    # DAILY STATCAST DATA FROM BASEBALL SAVANT
    df = get_statcast_data()
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table="{dataset}.statcast_{dt}".format(dataset=dataset, dt=yesterday.replace("-","")),
                      if_exists="replace")

    # DAILY STANDARD GAME LOGS
    df = utils.get_gamelog_range(date_range=[yesterday], game_log_type="batting")
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table="{dataset}.batting_{dt}".format(dataset=dataset, dt=yesterday.replace("-","")),
                      if_exists="replace")

     # DAILY STANDARD GAME LOGS
    df = utils.get_gamelog_range(date_range=[yesterday], game_log_type="pitching")
    pandas_gbq.to_gbq(df, project_id=project,
                       destination_table="{dataset}.pitching_{dt}".format(dataset=dataset, dt=yesterday.replace("-","")),
                       if_exists="replace")

    # DAILY INJURY REPORT FROM MLBAM
    df = utils.mlb_injuries()
    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.injuries_{dt}".format(dataset=dataset, dt=today.replace("-","")),
              if_exists="replace")

    # PROBABLE PITCHERS TODAY FROM MLBAM
    dt_split = [int(x) for x in today.split("-")]
    probables = utils.probablePitchers(dt_split[0],dt_split[1],dt_split[2])
    df = probables.run()
    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.probable_pitchers_{dt}".format(dataset=dataset, dt=dt.replace("-","")),
              if_exists="replace")


def seatgeek_events(request):
    # DEFINE ENVIRONMENT VARIABLES
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    seat_geek_client_id = os.environ["SEAK_GEEK_CLIENT_ID"]
    seat_geek_secret = os.environ["SEAT_GEEK_SECRET"]
    today = datetime.now().strftime("%Y-%m-%d")

    # DAILY INJURY REPORT FROM MLBAM
    geek = seatgeek(client=seat_geek_client_id, secret=seat_geek_secret)
    df =geek.run()
    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.tickets_{dt}".format(dataset=dataset, dt=today.replace("-","")),
              if_exists="replace")

def mlb_weather(request):
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    api_key = os.environ["WEATHER_API_KEY"]
    today = datetime.now().strftime("%Y-%m-%d")

    client = weather(project_id=project,
                     dataset=dataset,
                     date=today,
                     api_key=api_key)
    df = client.get_mlb_weather(darksky=True)
    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.weather_{dt}".format(dataset=dataset, dt=today.replace("-","")),
              if_exists="replace")


def starting_lineups(request):
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    today = datetime.now().strftime("%Y-%m-%d")

    starting_lineups = utils.startingLineups(project=project, dataset=dataset, dt = today)
    df = starting_lineups.run()
    if df is not None:
        pandas_gbq.to_gbq(df, project_id=project,
                  destination_table="{dataset}.starting_lineups_{dt}".format(dataset=dataset, dt=today.replace("-","")),
                  if_exists="replace")
