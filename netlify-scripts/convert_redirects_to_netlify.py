import argparse
import os
import pandas as pd
from utils import convert_redirect_format

def get_all_redirect_files(directory: str) -> list:
    """Gets a list of all files in a directory.

    Args:
    directory: The path to the directory as a string

    Returns:
    A list of file names
    """
    file_list = []
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            file_list.append(item)
    return file_list



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--file-name",
        default="all",
        help="Which redirect file to convert, defaults to all redirect files in the scraped-redirects/sorted dir",
        type=str,
    )
    args = parser.parse_args()
    file_name = args.file_name

    if file_name != "all":
        convert_redirect_format(file_name)
    else:
        redirect_file_list = get_all_redirect_files("scraped-redirects/sorted")
        for SOURCE_FILE in redirect_file_list:
            convert_redirect_format(SOURCE_FILE)


if __name__ == "__main__":
    main()
