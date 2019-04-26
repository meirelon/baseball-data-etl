import os
from datetime import datetime, timedelta
import pandas as pd
import pandas_gbq
from deps.statcast import get_statcast_data
from deps.utils import get_gamelog_range, probablePitchers, mlb_injuries, get_date_range_days, mlb_injuries

def mlb_daily_etl(request):
    project = os.environ["PROJECT_ID"]
    dataset = os.environ["DATASET"]
    today = datetime.now().strftime("%Y-%m-%d")
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    #DAILY STATCAST DATA FROM BASEBALL SAVANT
    df = get_statcast_data()
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table="{dataset}.statcast_{dt}".format(dataset=dataset, dt=yesterday.replace("-","")),
                      if_exists="replace")

    #DAILY STANDARD GAME LOGS
    df = get_gamelog_range([yesterday])
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table="{dataset}.batting_{dt}".format(dataset=dataset, dt=yesterday.replace("-","")),
                      if_exists="replace")

    #DAILY INJURY REPORT FROM MLBAM
    df = mlb_injuries()
    pandas_gbq.to_gbq(df, project_id=project,
              destination_table="{dataset}.injuries_{dt}".format(dataset=dataset, dt=today.replace("-","")),
              if_exists="replace")

    #PROBABLE PITCHERS OVER NEXT THREE DAYS FROM MLBAM
    date_range = get_date_range_days(start=0, end=2)
    for dt in date_range:
      dt_split = [int(x) for x in dt.split("-")]
      probables = probablePitchers(dt_split[0],dt_split[1],dt_split[2])
      df = probables.run()
      pandas_gbq.to_gbq(df, project_id=project,
                destination_table="{dataset}.probable_pitchers_{dt}".format(dataset=dataset, dt=dt.replace("-","")),
                if_exists="replace")
