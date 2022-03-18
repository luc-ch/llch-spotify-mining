import configparser

from . import _import_data
from ..utils.singleton_meta import SingletonMeta

from . import _countries


class Configuration(metaclass=SingletonMeta):
    """
    Loads the general configurations for the program to work properly
    """

    def __init__(self, config_file_path="config.ini", default_file_path="default.ini"):
        config_parser = self._read_config_file(config_file_path, default_file_path)
        self._setup_variables(config_parser)

    def _read_config_file(
        self, config_file_path="config.ini", default_file_path="default.ini"
    ):
        _import_data.copy_config_to_cwd()
        config_parser = configparser.ConfigParser()
        config_parser.read(default_file_path)
        config_parser.read(config_file_path)
        return config_parser

    def _setup_variables(self, config_parser: configparser.ConfigParser):
        self.db_connection_string = config_parser.get("db", "connection_string")
        self.db_host = config_parser.get("db", "host")
        self.db_port = config_parser.getint("db", "port")
        self.db_database = config_parser.get("db", "database")
        self.db_user = config_parser.get("db", "user")
        self.db_password = config_parser.get("db", "password")

        self.token_spotify_client_id = config_parser.get("token", "spotify_client_id")
        self.token_spotify_user_id = config_parser.get("token", "spotify_user_id")
        self.token_spotify_client_secret = config_parser.get(
            "token", "spotify_client_secret"
        )
        self.token_spotify_redirect_uri = config_parser.get(
            "token", "spotify_redirect_uri"
        )
        self.token_genius_secret = config_parser.get("token", "genius_secret")
        self.countries_dict = _countries.countries_dict

        self.general_persist_to_file = config_parser.getboolean(
            "general", "persist_to_file"
        )
        self.general_dataset_folder = config_parser.get("general", "dataset_folder")
