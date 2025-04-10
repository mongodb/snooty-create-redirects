import json
import sys
import boto3
import os





def get_all_redirect_files(directory: str)-> list:
    """Gets a list of all files in a directory.
    
    Args:
    directory: The path to the directory as a string
    
    Returns:
    A list of file names as a list
    """
    file_list = []
    for item in os.listdir(directory):
        item_path = os.path.join(directory, item)
        if os.path.isfile(item_path):
            file_list.append(item)
    return file_list


def normalize(origin: str, destination: str) -> tuple[str, str]:
    # Takes two strings which correspond to the origin of a redirect and the destination of a redirect
    # Remove prefixes and suffixes and assert that both strings begin with a slash
    # Returns two formatted strings

    # Remove https://www.mongodb.com prefix
    BASE = "https://www.mongodb.com/"
    docs_prefix = "docs"
    # remove index.html
    index_suffix = 'index.html'
    removal_candidates= [BASE, index_suffix]
    for substring in removal_candidates:
        origin = origin.replace(substring, "")
        destination = destination.replace(substring, "")


    # double check the format expected by netlify
    if not destination.startswith("/"):
        destination = "/"+destination
    
    if not origin.startswith("/"):
        origin = "/"+origin

    return origin, destination


def main() -> None:
    redirect_file_list = get_all_redirect_files("scraped-redirects/sorted")
    for SOURCE_FILE in redirect_file_list:
        output_rules = []
        with open(f"scraped-redirects/sorted/{SOURCE_FILE}", "r") as file:
            file = json.load(file)
            for redirect in file:
                origin, destination = normalize(redirect["origin"], redirect["destination"])
                ##add raw redirect comment back in??
                output_rules.append("\n[[redirects]] \rfrom = \""+ origin + "\"\rto = \""+ destination + "\"\r\r")

        txt_file = SOURCE_FILE.replace("json", "txt")
        DESTINATION_FILE = f"netlify-redirects/{txt_file}"
        with open(DESTINATION_FILE, "w") as f:
            f.write("".join(output_rules))

  



if __name__ == "__main__":
    main()