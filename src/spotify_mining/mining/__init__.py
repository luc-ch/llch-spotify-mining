from . import s01_extract_charts
from . import s03_extract_tracks
from . import s04_extract_lyrics


def run():
    print()
    print("s01_extract_charts")
    s01_extract_charts.run()
    print()
    print("s03_extract_tracks")
    s03_extract_tracks.run()
    print()
    print("s04_extract_lyrics")
    s04_extract_lyrics.run()
