import json
import pandas as pd
import requests
from tqdm.notebook import tqdm

from datetime import datetime

from IPython.display import display


from ...services import postgres

from .s03_extract_tracks_base import ExtractSpotifyBase


class ExtractSpotifyFeatures(ExtractSpotifyBase):
    def __init__(self):
        super().__init__()

    def run(self):
        command = """
                SELECT DISTINCT t.track_id
                FROM tracks t
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM track_features tf
                    WHERE t.track_id=tf.track_id
                )
                ORDER BY t.track_id;
        """
        processed = 0
        for track_ids in postgres.fetch_many_rows_from_postgres(command, 100):
            track_ids = [y for y in track_ids if len(y) == 22]
            if len(track_ids) == 0:
                continue
            processed += len(track_ids)
            print(processed, end=" ")
            df_track_features = self.get_features_subset(track_ids)
            if len(df_track_features) > 0:
                self._save_df_to_postgres(df_track_features)

    def _save_df_to_postgres(self, track_features):
        track_features.to_sql(
            "track_features", self.engine, if_exists="append", index=False
        )

    def get_features_subset(self, track_ids):
        aux_track_ids = ",".join(track_ids)
        path = f"{self.spotify_api_uri}/audio-features/?ids={aux_track_ids}"
        track_features = []
        try:
            r = requests.get(path, headers=self.get_payload_with_token())
            if not r.ok:
                raise RuntimeError(f"Falla _get_features_subset - {r.text}")
            j = json.loads(r.text)
            for f in j["audio_features"]:
                if f is not None:
                    track_features.append(self._get_features_individual(f))
        except Exception as e:
            print(f"Something went wrong: | {e} | {path}")
        return pd.DataFrame(track_features)

    def _get_features_individual(self, f):
        f_id = f["id"] if "id" in f else None
        f_danceability = f["danceability"] if "danceability" in f else None
        f_energy = f["energy"] if "energy" in f else None
        f_key = f["key"] if "key" in f else None
        f_loudness = f["loudness"] if "loudness" in f else None
        f_mode = f["mode"] if "mode" in f else None
        f_speechiness = f["speechiness"] if "speechiness" in f else None
        f_acousticness = f["acousticness"] if "acousticness" in f else None
        f_instrumentalness = f["instrumentalness"] if "instrumentalness" in f else None
        f_liveness = f["liveness"] if "liveness" in f else None
        f_valence = f["valence"] if "valence" in f else None
        f_tempo = f["tempo"] if "tempo" in f else None
        return {
            "track_id": f_id,
            "danceability": f_danceability,
            "energy": f_energy,
            "key": f_key,
            "loudness": f_loudness,
            "mode": f_mode,
            "speechiness": f_speechiness,
            "acousticness": f_acousticness,
            "instrumentalness": f_instrumentalness,
            "liveness": f_liveness,
            "valence": f_valence,
            "tempo": f_tempo,
        }

