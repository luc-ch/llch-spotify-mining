import json
import pandas as pd
import requests
from tqdm.notebook import tqdm

from datetime import datetime

from IPython.display import display


from ...services import postgres

from .s03_extract_tracks_base import ExtractSpotifyBase


class ExtractSpotifyArtists(ExtractSpotifyBase):
    def __init__(self):
        super().__init__()

    def run(self):
        command = """
                SELECT DISTINCT rta.artist_id
                FROM rel_tracks_artists rta
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM artists a
                    WHERE rta.artist_id=a.artist_id
                )
                ORDER BY rta.artist_id;
        """
        processed = 0
        for artist_ids in postgres.fetch_many_rows_from_postgres(command, 50):
            artist_ids = [y for y in artist_ids if len(y) == 22]
            if len(artist_ids) == 0:
                continue
            processed += len(artist_ids)
            print(processed, end=" ")
            df_artists = self._get_artists_subset(",".join(artist_ids))
            self._save_df_to_postgres(df_artists)

    def _save_df_to_postgres(self, df_artists):
        df_rel_artist_genre = (
            df_artists[["artist_id", "genres"]]
            .explode("genres")
            .rename(columns={"genres": "genre"})
            .dropna()
        )
        df_artists.to_sql("artists", self.engine, if_exists="append", index=False)
        df_rel_artist_genre.to_sql(
            "rel_artists_genres", self.engine, if_exists="append", index=False
        )

    def _get_artists_subset(self, aux_artists_ids):
        path = f"https://api.spotify.com/v1/artists/?ids={aux_artists_ids}"
        artist_basics = []
        try:
            r = requests.get(path, headers=self.get_payload_with_token())
            j = json.loads(r.text)

            for f in j["artists"]:
                x = {
                    "artist_id": f["id"],
                    "followers": f["followers"]["total"],
                    "genres": f["genres"],
                    "artist_name": f["name"],
                    "popularity": f["popularity"],
                }
                artist_basics.append(x)
        except Exception as e:
            print("Something went wrong: |", e, "|", path)
        return pd.DataFrame(artist_basics)

    def _get_artists(self):
        start = 0
        artist_max = 50
        artist_basics = []

        while start < len(self.new_list):
            print(start, end=" ")
            aux_artists_ids = ",".join(self.artist_ids[start : (start + artist_max)])
            artist_basics.extend(self._get_artists_subset(aux_artists_ids))
            # Make request
            start += artist_max
        return artist_basics

    def set_rel_artists_genres(self):
        self.df_rel_artists_genres = (
            self.df_artists[["artist_id", "genres"]]
            .explode("genres")
            .rename(columns={"genres": "genre"})
            .dropna()
        )
        self.df_rel_artists_genres.to_sql(
            "rel_artists_genres", self.engine, if_exists="append", index=False
        )

    def set_artists(self):
        command = """
                SELECT DISTINCT rta.artist_id
                FROM rel_tracks_artists rta
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM artists a
                    WHERE rta.artist_id=a.artist_id
                )
                ORDER BY rta.artist_id;
        """
        self.artist_ids = postgres.execute_command_postgres(command)
        self.new_list = [
            self.artist_ids[i : i + 50] for i in range(0, len(self.artist_ids), 50)
        ]
        artist_basics = self._get_artists()
        df_artists = pd.DataFrame(artist_basics)
        df_artists.to_sql("artists", self.engine, if_exists="append", index=False)

