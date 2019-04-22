import os
from datetime import datetime, timedelta
from deps.statcast import get_statcast_data
import pandas as pd
import pandas_gbq
from deps.utils import get_date_range, get_gamelog_range

def statcast_request(request):
    project = os.environ["PROJECT_ID"]
    destination_table = os.environ["DESTINATION_TABLE"]
    # key = os.environ["KEY"]
    df = get_statcast_data()
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table=destination_table+"_{}".format(datetime.now().strftime("%Y")),
                      if_exists="append")

def mlb_game_logs(request):
    project = os.environ["PROJECT_ID"]
    destination_table = os.environ["DESTINATION_TABLE"]
    yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')

    df = get_gamelog_range(get_date_range(start=yesterday))
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table=destination_table,
                      if_exists="append")
