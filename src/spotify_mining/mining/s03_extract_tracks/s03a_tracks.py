import json
import pandas as pd
import requests
from tqdm.notebook import tqdm

from datetime import datetime

from IPython.display import display


from ...services import postgres

from .s03_extract_tracks_base import ExtractSpotifyBase


class ExtractSpotifyTracks(ExtractSpotifyBase):
    def __init__(self):
        super().__init__()

    def run(self):
        command = """
                SELECT DISTINCT track_id
                FROM charts c
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM tracks t
                    WHERE c.track_id=t.track_id
                )
                ORDER BY track_id;
        """
        processed = 0
        for track_ids in postgres.fetch_many_rows_from_postgres(command, 50):
            track_ids = [y for y in track_ids if len(y) == 22]
            if len(track_ids) == 0:
                continue
            processed += len(track_ids)
            print(processed, end=" ")
            df_track_basics = self._get_tracks_data_subset(",".join(track_ids))
            self._save_df_to_postgres(df_track_basics)

    def _save_df_to_postgres(self, df_track_basics):
        df_track_basics.drop("aux_artist_ids", axis=1).to_sql(
            "tracks", self.engine, if_exists="append", index=False
        )
        df_rel_track_artist = (
            df_track_basics[["track_id", "aux_artist_ids"]]
            .explode("aux_artist_ids")
            .rename(columns={"aux_artist_ids": "artist_id"})
        )
        df_rel_track_artist.to_sql(
            "rel_tracks_artists", self.engine, if_exists="append", index=False
        )

    def _get_tracks_data_subset(self, aux_track_ids):
        path = f"{self.spotify_api_uri}/tracks/?ids={aux_track_ids}"
        track_basics = []
        try:
            r = requests.get(path, headers=self.get_payload_with_token())
            if not r.ok:
                raise RuntimeError(f"Falla _get_tracks_data_subset - {r.text}")
            j = json.loads(r.text)
            for f in j["tracks"]:
                track_basics.append(self._get_tracks_data_individual(f))
        except Exception as e:
            print(f"Something went wrong: | {e} | {path}")
        return pd.DataFrame(track_basics)

    def _get_tracks_data_individual(self, f):
        aux_artist_ids = []
        artists = []
        for x in f["artists"]:
            aux_artist_ids.append(x["id"])
            artists.append(x["name"])
        artist_ids = ",".join(aux_artist_ids)
        release_date_str = f["album"]["release_date"]
        try:
            if len(release_date_str) == 10:
                release_date = datetime.strptime(f["album"]["release_date"], "%Y-%m-%d")
            elif len(release_date_str) == 4 and release_date_str != "0000":
                release_date = datetime.strptime(f["album"]["release_date"], "%Y")
            elif len(release_date_str) == 7:
                release_date = datetime.strptime(f["album"]["release_date"], "%Y-%m")
            else:
                release_date = None
                raise RuntimeError(f"Intentó procesar la fecha {release_date_str}")
        except Exception:
            release_date = None
        return {
            "track_id": f["id"],
            "album_id": f["album"]["id"],
            "aux_artist_ids": aux_artist_ids,
            "artist_ids": artist_ids,
            "track_name": f["name"],
            "album_name": f["album"]["name"],
            "artist_name": artists,
            "duration_ms": f["duration_ms"],
            "album_release_date": release_date,
            "is_local": f["is_local"],
            "explicit": f["explicit"],
            "popularity": f["popularity"],
        }

