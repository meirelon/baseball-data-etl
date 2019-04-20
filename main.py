from deps.statcast import get_statcast_data
import pandas as pd

def df_to_bq(df, project, destination_table, key):
    df.to_gbq(project_id=project,
              destination_table=destination_table,
              if_exists='append')

def statcast_request(request):
    project = os.environ["PROJECT_ID"]
    destination_table = os.environ["DESTINATION_TABLE"]
    key = os.environ["KEY"]
    df = get_statcast_data()
    df_to_gbq(df, project, destination_table)
