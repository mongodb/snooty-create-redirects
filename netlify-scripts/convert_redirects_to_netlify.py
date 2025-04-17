import argparse
import json
import os


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


def normalize(origin: str, destination: str) -> tuple[str, str]:
    """Normalize the origin and destination of a redirect pair.

    Args:
    origin: origin string of a redirect
    destination: destination string of the redirect

    Returns:
    The normalized redirect as a tuple
    """

    BASE = "https://www.mongodb.com/"
    docs_prefix = "docs"
    index_suffix = "index.html"
    removal_candidates = [BASE, index_suffix, docs_prefix]
    for substring in removal_candidates:
        origin = origin.replace(substring, "")
        destination = destination.replace(substring, "")

    # TODO: double check the format expected by netlify
    if not destination.startswith("/"):
        destination = "/" + destination

    if not origin.startswith("/"):
        origin = "/" + origin

    return origin, destination


def convert(source_file_name: str):
    """Converts redirect tuples to Netlify format

    Args:
    origin: origin string of a redirect
    destination: destination string of the redirect

    Returns:
    The normalized redirect as a tuple
    """
    output_rules = []
    with open(f"scraped-redirects/sorted/{source_file_name}", "r") as file:
        file = json.load(file)
        for redirect in file:
            origin, destination = normalize(redirect["origin"], redirect["destination"])
            ## Add a comment on what the raw redirect was??
            output_rules.append(
                '\n[[redirects]] \rfrom = "'
                + origin
                + '"\rto = "'
                + destination
                + '"\r\r'
            )

    txt_file = source_file_name.replace("json", "txt")
    DESTINATION_FILE = f"netlify-redirects/{txt_file}"
    with open(DESTINATION_FILE, "w") as f:
        f.write("".join(output_rules))


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
        convert(file_name)
    else:
        redirect_file_list = get_all_redirect_files("scraped-redirects/sorted")
        for SOURCE_FILE in redirect_file_list:
            convert(SOURCE_FILE)


if __name__ == "__main__":
    main()
