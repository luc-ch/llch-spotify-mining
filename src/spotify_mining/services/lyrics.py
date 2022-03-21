import pandas as pd
import requests
from bs4 import BeautifulSoup
import re
import unidecode
from datetime import datetime
from urllib3.util import Retry
from tqdm.auto import tqdm

from ..utils.http_adapter import TimeoutHTTPAdapter


class GetLyrics:
    def __init__(self, genius_key):
        self.genius_key = genius_key
        self.names_for_dict = [
            "track_id",
            "track_name",
            "artists",
            "lyrics",
            "url",
            "retrieved_at",
        ]
        self.retries = Retry(
            total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504]
        )
        self.base_url = "https://api.genius.com"

    def get_new_http_session(self):
        http = requests.Session()
        http.mount("https://", TimeoutHTTPAdapter(max_retries=self.retries))
        return http

    def get_track_names(self):
        track_names = []
        for song in range(len(self.playlist["items"])):
            track_names.append(self.playlist["items"][song]["track"]["name"])
        self.track_names = track_names
        return self.track_names

    def get_track_artists(self):
        track_artists = []
        for song in range(len(self.playlist["items"])):
            track_artists.append(
                self.playlist["items"][song]["track"]["artists"][0]["name"]
            )
        self.track_artists = track_artists
        return self.track_artists

    def request_song_info(self, track_name, track_artist):
        self.track_name = track_name
        self.track_artist = track_artist
        params = {"access_token": self.genius_key}
        headers = {
            "User-Agent": "CompuServe Classic/1.22",
            "Accept": "application/json",
        }
        search_url = self.base_url + "/search"
        query = str(track_name) + " " + str(track_artist).replace(",", " ")

        with self.get_new_http_session() as http:
            page = http.get(f"{search_url}?q={query}", params=params, headers=headers)
            if not page.ok:
                self.response = None
                return None
            self.response = page.json()
        return self.response

    def check_hits(self):
        json = self.response
        if json is None:
            self.remote_song_info = None
            return json
        remote_song_info = None
        for hit in json["response"]["hits"]:
            if (
                self.track_artist.lower()
                in hit["result"]["primary_artist"]["name"].lower()
            ):
                remote_song_info = hit
                break
        self.remote_song_info = remote_song_info
        return self.remote_song_info

    def get_url(self):
        song_url = self.remote_song_info["result"]["url"]
        self.song_url = song_url
        return self.song_url

    def scrape_lyrics(self):
        page = requests.get(self.song_url)
        if not page.ok:
            return None
        html = BeautifulSoup(page.text, "html.parser")
        lyrics1 = html.find("div", {"id": "lyrics-root-pin-spacer"})
        if lyrics1 is not None:
            lyrics1 = lyrics1.find("div", {"data-lyrics-container": "true"})
        lyrics2 = html.find("div", class_="Lyrics__Container-sc-1ynbvzw-2 jgQsqn")
        if lyrics1:
            lyrics = lyrics1.get_text(separator=" ")
        elif lyrics2:
            lyrics = lyrics2.get_text(separator=" ")
        elif lyrics1 is None and lyrics2 is None:
            return None
        return lyrics[: min(8192, len(lyrics))]

    def get_lyrics_from_playlist(self):
        self.get_playlist_info()
        self.get_track_names()
        track_artists = self.get_track_artists()
        song_lyrics = []
        for i, (name, artist) in enumerate(zip(self.track_names, track_artists)):
            print(f'Working on track {i} - "{name}" - "{artist}".')
            self.request_song_info(name, artist)
            remote_song_info = self.check_hits()
            if remote_song_info is None:
                lyrics = None
                print(f"Track {i} is not in the Genius database.")
            else:
                self.get_url()
                lyrics = self.scrape_lyrics()
                if lyrics is None:
                    print(f"Track {i} is not in the Genius database.")
                else:
                    print(f"Retrieved track {i} lyrics!")
            song_lyrics.append(lyrics)
        return song_lyrics

    def get_single_lyric(self, name, artist):
        name = unidecode.unidecode(name)
        artist = unidecode.unidecode(artist)

        name = re.sub("[\(\[].*?[\)\]]", "", name).split("-", 1)[0].strip()
        artist = re.sub("[^\w,. ]", "", artist.replace("&", "and")).strip()
        err = None
        lyrics = None
        self.request_song_info(name, artist)
        remote_song_info = self.check_hits()
        if remote_song_info is None:
            if len(artist.rsplit(",", 1)) == 1:
                # Track not in Genius db 3.
                err = 3
                return lyrics, err, None
            # Track not in Genius db 1.
            err = 1
            artist = artist.rsplit(",", 1)[0]
            lyrics, err, url = self.get_single_lyric(name, artist)
        else:
            url = self.get_url()
            lyrics = self.scrape_lyrics()
            if lyrics is None:
                # Track not in Genius db 2.
                err = 2
            else:
                # Retrieved lyrics!!!!!!!!
                err = 0
        return lyrics, err, url

    def get_lyrics_in_dict(self, id, name, artist):
        lyrics, _, url = self.get_single_lyric(name, artist)
        return dict(
            zip(self.names_for_dict, (id, name, artist, lyrics, url, datetime.now()))
        )

    def get_single_lyric_wrapper(self, row):
        try:
            return pd.Series(self.get_single_lyric(row["track_name"], row["artists"]))
        except Exception:
            return None, None, None

    def get_lyrics_from_df(self, df):
        df2 = df.copy()
        tqdm.pandas()
        df2[["lyrics", "err", "url"]] = df2.progress_apply(
            self.get_single_lyric_wrapper, axis=1
        )
        return df2

        # https://medium.com/swlh/how-to-leverage-spotify-api-genius-lyrics-for-data-science-tasks-in-python-c36cdfb55cf3
        # function to scrape lyrics from genius

