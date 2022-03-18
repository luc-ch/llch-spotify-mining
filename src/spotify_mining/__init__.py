from .configurations import Configuration

from . import mining


def main(config_file):
    Configuration(config_file)
    mining.run()


def run_only_extract_lyrics(config_file):
    Configuration(config_file)
    mining.s04_extract_lyrics.run()
