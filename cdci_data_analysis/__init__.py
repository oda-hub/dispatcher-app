import os

from importlib.metadata import version, PackageNotFoundError


__author__ = "Andrea Tramacere"

try:
    __version__ = version(__name__)
except PackageNotFoundError:
    __version__ = "0.0.0"

__version__ += os.getenv("DISPATCHER_EXTRA_VERSION", "")


conf_dir = os.path.dirname(__file__)+'/config_dir'
