import os
from deps.statcast import get_statcast_data
import pandas as pd
import pandas_gbq

def statcast_request(request):
    project = os.environ["PROJECT_ID"]
    destination_table = os.environ["DESTINATION_TABLE"]
    # key = os.environ["KEY"]
    df = get_statcast_data()
    df.columns = [c.replace(".", "_" ) for c in df.columns]
    pandas_gbq.to_gbq(df, project_id=project,
                      destination_table=destination_table,
                      if_exists="append")
