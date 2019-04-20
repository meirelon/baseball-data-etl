import os
from deps.statcast import get_statcast_data
import pandas as pd

def statcast_request(request):
    project = os.environ["PROJECT_ID"]
    destination_table = os.environ["DESTINATION_TABLE"]
    # key = os.environ["KEY"]
    df = get_statcast_data()
    df.to_gbq(project_id=project,
              destination_table=destination_table,
              if_exists='append')
