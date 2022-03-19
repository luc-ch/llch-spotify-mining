from .configurations import Configuration

from . import mining


def main(config_file):
    Configuration(config_file)
    mining.run()

