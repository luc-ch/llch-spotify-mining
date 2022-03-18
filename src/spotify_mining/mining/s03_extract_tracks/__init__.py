from . import s03a_tracks
from . import s03b_artists
from . import s03c_features
from . import s03d_audio_analysis


def run():
    print()
    print("ExtractSpotifyTracks")
    s03a_tracks.ExtractSpotifyTracks().run()
    print()
    print("ExtractSpotifyArtists")
    s03b_artists.ExtractSpotifyArtists().run()
    print()
    print("ExtractSpotifyFeatures")
    s03c_features.ExtractSpotifyFeatures().run()
    print()
    print("ExtractSpotifyAudioAnalysis")
    s03d_audio_analysis.ExtractSpotifyAudioAnalysis().run()
