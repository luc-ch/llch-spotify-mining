"""Import the text/binary data that accompanies this package."""

import importlib.resources as pkg_resources

from . import templates

def copy_config_to_cwd():
    """
    Copy default.ini to current working directory 
    """
    file = "default.ini"
    copy_text_to_cwd(file)


def copy_text_to_cwd(file):
    """
    Copy text file to current working directory 
    """
    file_data = pkg_resources.read_text(templates, file)
    with open(file, "w") as f_out:
        f_out.write(file_data)
