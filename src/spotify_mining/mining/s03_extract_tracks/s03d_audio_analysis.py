import json
import pandas as pd
import requests
from tqdm.notebook import tqdm

from datetime import datetime

from IPython.display import display


from ...services import postgres

from .s03_extract_tracks_base import ExtractSpotifyBase


class ExtractSpotifyAudioAnalysis(ExtractSpotifyBase):
    def __init__(self):
        super().__init__()

    def run(self):
        command = """
                SELECT DISTINCT t.track_id
                FROM tracks t
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM audio_analysis aa
                    WHERE t.track_id=aa.track_id
                )
                ORDER BY t.track_id DESC;
        """
        processed = 0
        for track_id in postgres.fetch_rows_by_one_from_postgres(command):
            if len(track_id) != 22:
                continue
            processed += 1
            if processed % 10 == 0:
                print(processed, end=" ")
            df_audio_analysis = self.get_audio_analysis(track_id)
            if df_audio_analysis is not None:
                self._save_df_to_postgres(df_audio_analysis)

    def _save_df_to_postgres(self, audio_analysis):
        audio_analysis.to_sql(
            "audio_analysis", self.engine, if_exists="append", index=False
        )

    def get_audio_analysis(self, track_id):
        path = "https://api.spotify.com/v1/audio-analysis/" + track_id
        r = requests.get(path, headers=self.get_payload_with_token())
        df = None
        if not r.ok:
            print(f"FALLA {track_id}")
            # raise RuntimeError("Falla get_audio_analysis")
            return None
        j = json.loads(r.text)["segments"]
        #     print(j)
        df = pd.DataFrame.from_dict(j)
        df["track_id"] = track_id
        return self.pitches_timbre_new_columns(df)

    def pitches_timbre_new_columns(self, df_m2):
        df1 = pd.DataFrame(
            df_m2["pitches"].tolist(), columns=self.pitches_nombre, index=df_m2.index
        )
        df_m2 = pd.concat([df_m2, df1], axis=1)

        df1 = pd.DataFrame(
            df_m2["timbre"].tolist(), columns=self.timbre_nombre, index=df_m2.index
        )
        df_m2 = pd.concat([df_m2, df1], axis=1).drop(["pitches", "timbre"], 1)
        return df_m2
