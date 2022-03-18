from importlib_metadata import version, PackageNotFoundError

try:
    __version__ = version("llch_logger")
except PackageNotFoundError as e:
    __version__ = "unpackaged"
