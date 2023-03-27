import os
import sys


def is_valid_file(file_path) -> bool:
    """ method to check if a file path is valid or not

    Args:
        file_path (str): path to file

    Raises:
        FileNotFoundError
    """
    try:
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"The file {file_path} does not exist!")
        return True

    except FileNotFoundError:
        print("File path not valid")
        sys.exit(1)
