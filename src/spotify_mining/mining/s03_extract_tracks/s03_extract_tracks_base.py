import json
import pandas as pd
import requests
from requests.auth import HTTPBasicAuth
from tqdm.notebook import tqdm

import spotipy
import spotipy.util as util
from datetime import datetime

from sqlalchemy import create_engine

from IPython.display import display


from ...services import postgres
from ...configurations import Configuration


class ExtractSpotifyBase:
    def __init__(self):
        config = Configuration()
        self.engine = create_engine(config.db_connection_string)
        postgres.test_db_connection()
        self.client_id = config.token_spotify_client_id
        self.client_secret = config.token_spotify_client_secret
        self.user_id = config.token_spotify_user_id
        self.path = "https://accounts.spotify.com/api/token"
        self.spotify_api_uri = "https://api.spotify.com/v1"
        self.payload = None
        self.payload_basic = {"grant_type": "client_credentials"}
        self.scope = "user-library-read"
        self.redirect_uri = config.token_spotify_redirect_uri
        self.token_reuse = 0
        self.test_request()

        self.pitches_nombre = [
            "p00_c",
            "p01_cs",
            "p02_d",
            "p03_ds",
            "p04_e",
            "p05_f",
            "p06_fs",
            "p07_g",
            "p08_gs",
            "p09_a",
            "p10_as",
            "p11_b",
        ]

        self.timbre_nombre = [
            "t00",
            "t01",
            "t02",
            "t03",
            "t04",
            "t05",
            "t06",
            "t07",
            "t08",
            "t09",
            "t10",
            "t11",
        ]

    def test_request(self):
        r = requests.post(
            self.path,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            data=self.payload_basic,
        )

        if not r.ok:
            raise RuntimeError(f"Something happened - {r}")

    def get_spotify_object(self,):
        token = util.prompt_for_user_token(
            username=self.user_id,
            scope=self.scope,
            client_id=self.client_id,
            client_secret=self.client_secret,
            redirect_uri=self.redirect_uri,
        )
        if token:
            sp = spotipy.Spotify(auth=token)
        else:
            print("Can't get token for username.")
        return sp

    def get_payload_with_token(self):
        self.token_reuse += 1
        if self.token_reuse < 50 and self.payload is not None:
            return self.payload
        self.token_reuse = 0
        aux_r = requests.post(
            self.path,
            auth=HTTPBasicAuth(self.client_id, self.client_secret),
            data=self.payload_basic,
        )
        if not aux_r.ok:
            raise RuntimeError("Falla get_payload_with_token")
        d = json.loads(aux_r.text)
        self.payload = {"Authorization": d["token_type"] + " " + d["access_token"]}
        return self.payload

