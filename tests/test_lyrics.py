import unittest

from spotify_mining.services.lyrics import GetLyrics
from spotify_mining.configurations import Configuration


class TestLyrics(unittest.TestCase):
    tracks = [
        (
            "550erGcdD9n6PnwxrvYqZT",
            "Hey Stephen (Taylor’s Version)",
            '{"Taylor Swift"}',
        ),
        ("21Dt0T82tst15r3HCIM2Ku", "No Te Vas", "{Nacho}"),
        ("2p8IUWQDrpjuFltbdgLOag", "After Hours", '{"The Weeknd"}'),
        ("0PabDDMMzwmn9cfJsfes7W", "Chica Bombastic", '{"Wisin & Yandel"}'),
        ("2ijef6ni2amuunRoKTlgww", "Sin Pijama", '{"Becky G","Natti Natasha"}'),
        (
            "xxx",
            "AAAAAAAAAAAAAAAAAA",
            "AAAAAAAAAAAAAAAAAA",
        ),  # ESTE ULTIMO DEVUELVE ERROR
    ]

    def test_lyrics_by_one(self):
        config = Configuration("config.ini")
        songs = GetLyrics(config.token_genius_secret)
        lyrics_letter = []
        for row in self.tracks:
            lyrics_dict = songs.get_lyrics_in_dict(row[0], row[1], row[2])
            lyrics_letter.append(lyrics_dict["lyrics"])
        print([x[:20] for x in lyrics_letter if x is not None])
        assert sum(x is not None for x in lyrics_letter) > 3
        assert sum(x is not None for x in lyrics_letter) < len(self.tracks)


if __name__ == "__main__":
    unittest.main()
