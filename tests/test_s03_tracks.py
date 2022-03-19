import unittest
from IPython.display import display

from spotify_mining.mining import s03_extract_tracks
from spotify_mining.configurations import Configuration


class TestTracks(unittest.TestCase):
    track_ids = [
        "0jT8Nl0shPS8115is0wD2Q",
        "0SqqAgdovOE24BzxIClpjw",
        "2DEZmgHKAvm41k4J3R2E9Y",
        "0pgj4EzB1XRqgZemoMNG5D",
        "7na7Bk98usp84FaOJFPv3d",
    ]
    artist_ids = [
        "28gNT5KBp7IjEOQoevXf9N",
        "4q3ewBCX7sLwd24euuV69X",
        "28gNT5KBp7IjEOQoevXf9N",
        "52qzWdNUp6ebjcNsvgZSiC",
        "1mcTU81TzQhprhouKaTkpq",
    ]
    Configuration()

    def test_a_tracks_data(self):
        extractor = s03_extract_tracks.s03a_tracks.ExtractSpotifyTracks()
        df = extractor.get_tracks_data_subset(self.track_ids)
        display(df.head(10))
        assert len(df) == len(self.track_ids)

    def test_b_artists_data(self):
        extractor = s03_extract_tracks.s03b_artists.ExtractSpotifyArtists()
        df = extractor.get_artists_subset(self.artist_ids)
        display(df.head(10))
        assert len(df) == len(self.artist_ids)

    def test_c_features(self):
        extractor = s03_extract_tracks.s03c_features.ExtractSpotifyFeatures()
        df = extractor.get_features_subset(self.track_ids)
        display(df.head(10))
        assert len(df) == len(self.track_ids)

    def test_d_audio_analysis(self):
        extractor = s03_extract_tracks.s03d_audio_analysis.ExtractSpotifyAudioAnalysis()
        df = extractor.get_audio_analysis(self.track_ids[0])
        display(df.head(10))
        assert len(df) > 0


if __name__ == "__main__":
    unittest.main()
