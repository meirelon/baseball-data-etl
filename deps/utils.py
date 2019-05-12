import requests
from datetime import datetime, timedelta
import pandas as pd
from pybaseball import batting_stats_range

def get_date_range(start="2019-03-15", end=datetime.now().strftime("%Y-%m-%d")):
  return [x.strftime("%Y-%m-%d") for x in pd.date_range(start=start, end=end)]

def get_date_range_days(start, end):
  start = (datetime.now() + timedelta(days=start)).strftime("%Y-%m-%d")
  end = (datetime.now() + timedelta(days=end)).strftime("%Y-%m-%d")
  return [d.strftime("%Y-%m-%d") for d in pd.date_range(start, end)]

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


class probablePitchers:
    """
    NOTE: This is still using the legacy MLBAM link: gd2.mlb.com
    Migration to new MLBAM stats api is a work in progress:
    Example: http://statsapi.mlb.com//api/v1/game/529572/boxscore
    """
    def __init__(self, year, month, day):
        self.year = int(year)
        self.month = int(month)
        self.day = int(day)

    def _get_mlb_game_ids(self):
        import mlbgame
        mlb_games = mlbgame.day(self.year, self.month, self.day)
        return [g.game_id for g in mlb_games]


    def _get_date_from_game_id(self, game_id):
        year, month, day, _discard = game_id.split('_', 3)
        return int(year), int(month), int(day)

    def _get_overview(self, game_id):
        BASE_URL = ('http://gd2.mlb.com/components/game/mlb/'
                  'year_{0}/month_{1:02d}/day_{2:02d}/')
        GAME_URL = BASE_URL + 'gid_{3}/{4}'
        """Return the linescore file of a game with matching id."""
        year, month, day = self._get_date_from_game_id(game_id)
        try:
          url=GAME_URL.format(year, month, day, game_id,
                                           'linescore.json')
          r = requests.get(url)
          return r.json().get("data").get("game")
        except HTTPError:
          raise ValueError('Could not find a game with that id.')

    def _get_probable_pitchers(self, game_id):
        game_json = self._get_overview(game_id)
        away_df = pd.DataFrame(game_json.get("away_probable_pitcher"),index=[0])
        away_df["tm"] = game_json.get("away_name_abbrev")
        away_df["is_home"] = False
        home_df = pd.DataFrame(game_json.get("home_probable_pitcher"),index=[0])
        home_df["tm"] = game_json.get("home_name_abbrev")
        home_df["is_home"] = True
        return pd.concat([away_df, home_df],axis=0,ignore_index=True)


    def run(self):
        date = "{}-{}-{}".format(self.year, self.month, self.day)
        cols = ["first_name", "last_name", "id", "tm", "is_home"]
        probable_pitchers_list = [self._get_probable_pitchers(id)
                                    for id in self._get_mlb_game_ids()]
        probable_pitcher_df = pd.concat(probable_pitchers_list,
                                        axis=0,
                                        ignore_index=True,
                                        sort=True)[cols]
        probable_pitcher_df["date"] = datetime.strptime(date, "%Y-%m-%d").strftime("%Y-%m-%d")
        return probable_pitcher_df


class startingLineups:
    def __init__(self, project, dataset, dt):
        self.project = project
        self.dataset = dataset
        self.dt = dt

    def get_games_by_date(self):

        #get the games from 2019 schedule
        q = """select game_pk
        from(
        select *, row_number() over(partition by home order by game_pk) as r
        from(
        select regexp_extract(link, "[0-9]{2,}") as game_pk, home
        from `%s.%s.schedule_2019`
        where date(timestamp(gameDate)) = date(timestamp('%s'))
        group by 1,2
        )
        )
        where r = 1"""

        query_formatted = q % (self.project, self.dataset, self.dt)

        df = pd.read_gbq(project_id=self.project, query=query_formatted, dialect="standard")
        return df

    def get_starting_lineups(self, game_pk):
        url = "https://statsapi.mlb.com/api/v1/schedule?gamePk={game_pk}&language=en&hydrate=lineups"
        r = requests.get(url.format(game_pk=game_pk))
        r_json = r.json()

        lineups = r_json.get("dates")[0].get("games")[0].get("lineups")

        if lineups is not None:
            away = lineups.get("awayPlayers")
            home = lineups.get("homePlayers")
            if away is not None and home is not None:
                away_lineup = pd.DataFrame(pd.concat([pd.DataFrame(x) for x in away if x is not None], axis=0, ignore_index=True).drop_duplicates(keep=False)["id"].unique(), columns = ["id"])
                away_lineup["is_home"] = False
                away_lineup["batting_order"] = [x for x in range(1,10)]
                home_lineup = pd.DataFrame(pd.concat([pd.DataFrame(x) for x in home if x is not None], axis=0, ignore_index=True).drop_duplicates(keep=False)["id"].unique(), columns = ["id"])
                home_lineup["is_home"] = True
                home_lineup["batting_order"] = [x for x in range(1,10)]
                lineup_df = pd.concat([away_lineup, home_lineup], axis=0)

                return lineup_df

    def run(self):
        game_pks_df = self.get_games_by_date()
        starting_lineup_df = pd.concat([self.get_starting_lineups(x)
                                        for x in game_pks_df.game_pk.values if x is not None], axis=0, ignore_index=True)
        starting_lineup_df["date"] = self.dt
        return starting_lineup_df
