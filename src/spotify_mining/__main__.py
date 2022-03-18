import argparse

from . import main

from ._version import __version__

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description=f"""
    This module mines Spotify data [spotify-mining {__version__}]
    """
    )
    parser.add_argument(
        "-m",
        "--message",
        type=str,
        help="Message to be sent. By default, it is of type information.",
    )
    parser.add_argument(
        "--config",
        type=str,
        default="./config.ini",
        help="Location of the configuration file. Default: ./config.ini",
    )
    args = parser.parse_args()

    main(args.config)
