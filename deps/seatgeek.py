import requests
import pandas as pd

class seatgeek:
    def __init__(self, client, secret):
        self.client = client
        self.secret = secret

    def get_mlb_events(self, venues):
        events_q = "https://api.seatgeek.com/2/events?client_id={MYCLIENTID}&client_secret={MYCLIENTSECRET}&venue.id={VENUES}"
        r = requests.get(events_q.format(MYCLIENTID=self.client,
                                         MYCLIENTSECRET=self.secret,
                                         VENUES=venues))
        return r.json().get("events")

    def get_home_venue_id(self, team):
        slug_url = "https://api.seatgeek.com/2/performers?client_id={MYCLIENTID}&client_secret={MYCLIENTSECRET}&slug={TEAM}"
        r = requests.get(slug_url.format(MYCLIENTID=self.client,
                                         MYCLIENTSECRET=self.secret,
                                         TEAM="-".join(team.split(" "))))
        return r.json().get("performers")[0].get("home_venue_id")

    def get_mlb_venues(self):
        mlb_teams = ["baltimore orioles",
         "arizona diamondbacks",
         "boston red sox",
         "chicago white sox",
         "cleveland indians",
         "detroit tigers",
         "houston astros",
         "kansas city royals",
         "los angeles angels",
         "minnesota twins",
         "new york yankees",
        "oakland athletics",
        "seattle mariners",
        "tampa bay rays",
         "texas rangers",
        "toronto blue jays",
        "atlanta braves",
         "chicago cubs",
         "cincinnati reds",
         "colorado rockies",
         "los angeles dodgers",
         "miami marlins",
         "milwaukee brewers",
         "new york mets",
         "philadelphia phillies",
         "pittsburgh pirates",
         "san diego padres",
         "san francisco giants",
         "st louis cardinals",
         "washington nationals"]
        mlb_venues = {t:self.get_home_venue_id(t) for t in mlb_teams}
        return ",".join([str(v) for v in mlb_venues.values()])

    def get_ticket_info(self):
        mlb_venues = self.get_mlb_venues()
        mlb_events = self.get_mlb_events(mlb_venues)
        tickets = {s.get("title"):{"stats":s.get("stats"),
                                     "popularity":s.get("popularity"),
                                     "score":s.get("score"),
                                     "lat":[x for x in s.get("venue").get("location").values()][0],
                                     "long":[x for x in s.get("venue").get("location").values()][1]} for s in mlb_events}
        return tickets

    def get_event_df(self, events, event):
        if len(event.split(" at ")) > 1:
            df = pd.DataFrame(events[event].get("stats")).iloc[0:1,:]
            df["popularity"] = events[event].get("popularity")
            df["score"] = events[event].get("score")
            df["lat"] = events[event].get("lat")
            df["long"] = events[event].get("long")
            df["home"] = event.split(" at ")[1]
            df["away"] = event.split(" at ")[0]
            return df

    def run(self):
        ticket_info = self.get_ticket_info()
        return pd.concat([self.get_event_df(ticket_info, k) for k in list(ticket_info.keys())], axis=0)
