#!/usr/bin/env python
# coding: utf-8
import pandas as pd
from sqlalchemy import create_engine

from ..configurations import Configuration

from ..services import lyrics
from ..services import postgres


class ExtractLyrics:
    def __init__(self):
        config = Configuration()
        self.engine = create_engine(config.db_connection_string)
        postgres.test_db_connection()
        self.base_url = "http://api.genius.com/"
        self.access_token = config.token_genius_secret
        self.songs = lyrics.GetLyrics(self.access_token,)

    def run(self):
        command = """
                SELECT DISTINCT t.track_id, t.track_name, t.artist_name
                FROM tracks t
                WHERE NOT EXISTS (
                    SELECT 1 
                    FROM lyrics l
                    WHERE t.track_id=l.track_id
                ) ORDER BY 1;
        """
        processed = 0
        for row in postgres.fetch_rows_by_one_from_postgres(command):
            if len(row["track_id"]) != 22:
                continue
            processed += 1
            if processed % 10 == 0:
                print(processed, end=" ")
            lyrics_dict = self.songs.get_lyrics_in_dict(
                row["track_id"], row["track_name"], row["artist_name"]
            )
            df_lyrics = pd.DataFrame([lyrics_dict])
            self._save_df_to_postgres(df_lyrics)

    def _save_df_to_postgres(self, df_lyrics):
        df_lyrics.to_sql("lyrics", self.engine, if_exists="append", index=False)


def run():
    ExtractLyrics().run()
