import argparse
from datetime import datetime
import pandas as pd
from pybaseball import statcast


def getStatCastData():
    now = datetime.now().strftime("%Y-%m-%d")
    return statcast(now, verbose=False)

def main(argv=None):
    parser = argparse.ArgumentParser()

    parser.add_argument('--project_id',
                        dest='project_id',
                        default=None,
                        help='gcp project')
    parser.add_argument('--destination_table',
                        dest='destination_table',
                        default=None,
                        help='destination of table')
    parser.add_argument('--credentials',
                        dest='credentials',
                        default=None,
                        help='bq credentials')

    args, _ = parser.parse_known_args(argv)
    df = getStatCastData()

    import json
    with open(args.credentials, 'r') as f:
        creds = json.load(f)


    df.to_gbq(project_id=args.project_id,
                                 destination_table=args.destination_table,
                                 if_exists="append",
                                 credentials=creds,
                                 chunksize=1000)

if __name__ == '__main__':
    main()
