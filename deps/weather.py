import requests
import pandas as pd

class weather:
    def __init__(self, project_id, dataset, date, api_key):
        self.project_id = project_id
        self.dataset = dataset
        self.date = date
        self.api_key = api_key


    def get_weather_darksky(self, lat, long, time_string=None):
        weather_url = "https://api.darksky.net/forecast/{key}/{lat},{long}".format(key=self.api_key,
                                                                                   lat=lat,
                                                                                   long=long)
        if time_string is not None:
            weather_url = "https://api.darksky.net/forecast/{key}/{lat},{long},{t}".format(key=self.api_key,
                                                                                          lat=lat,
                                                                                          long=long,
                                                                                          t=time_string)
        r = requests.get(weather_url)
        return r.json().get("currently")


    def get_weather_gov(self, lat, long):
        """
        This api is free!
        """
        weather_url = "https://api.weather.gov/points/{lat},{long}".format(lat=lat, long=long)
        r = requests.get(weather_url)
        forecast = r.json().get("properties").get("forecast")
        forecast_request = requests.get(forecast).json()
        periods = forecast_request.get("properties").get("periods")
        weather_df = pd.concat([pd.DataFrame(p, index=[0]) for p in periods], axis=0, ignore_index=True)
        return  weather_df


    def get_query(self):
        q = """select date(timestamp(gameDate)) as date, away, home, latitude, longitude
                from `{project}.{dataset}.schedule_2019` a
                join `{project}.{dataset}.mlb_stadiums` b
                on lower(a.home) = lower(b.team_name)
                where cast(date(timestamp(gameDate)) as string) = "{date}"
                group by 1,2,3,4,5
                order by home"""

        return q.format(project=self.project_id, dataset=self.dataset, date=self.date)

    def get_mlb_weather(self, darksky=False):
        df = pd.read_gbq(project_id=self.project_id, query=self.get_query(), dialect="standard")
        if darksky:
            weather_df = pd.concat([pd.DataFrame(self.get_weather_darksky(x,y), index=[0])
                                    for x,y in zip(df["latitude"], df["longitude"])], axis=0, ignore_index=True)
        else:
            weather_df = pd.concat([pd.DataFrame(self.get_weather_gov(x,y), index=[0])
                                    for x,y in zip(df["latitude"], df["longitude"])], axis=0, ignore_index=True)
        return pd.concat([df, weather_df], axis=1)
