import requests
import json
import pandas as pd



## Assumes that each path has a leading slash, otherwise index from 2
def get_branch(redirect: str, index = 3):
    try:
        branch = redirect.split("/")[index]
    except IndexError as e: 
        print(redirect, branch, index)
    return branch


def add_path_placeholder(path: str, branch_index: int, replacement = ":version"):
    branch = get_branch(path, branch_index)
    new_path = path.replace(branch, replacement)
    return new_path

## Adds ":version" in place of the branch name
def add_placeholder(redirect: tuple, branch_index: int, replacement: str):
    return add_path_placeholder(redirect[0], branch_index, replacement), add_path_placeholder(redirect[1], branch_index, replacement)

# def separate_page_levels(redirects: list, version: str):
#     version_page_levels = []
#     to_specific_manual_page = []
#     prefix = f"/docs/{version}"
#     for redirect in redirects:
#         if redirect[1].startswith(prefix):
#             version_page_levels.append(redirect)
#         else:
#             to_specific_manual_page.append(redirect)
#     return version_page_levels, to_specific_manual_page
    


#  takes a redirect pair in the form of a list of two strings
def replace_version_placeholder(redirect: list[str], replacement = "v8.1"):
    return (redirect[0].replace(":version", replacement).strip(), redirect[1].replace(":version", replacement).strip())
    


# def remove_wildcard_captures(wildcard_list: list[list[str]], redirects_list: list[tuple])-> list[str]:
#     wildcards= ((list([*map(replace_version,wildcard_list)])))
    
#     for origin, destination in set(wildcards):
#         found = False
#         # if the origin is found in one of the consolidated redirects, pop that redirect from consolidated redirects
#         for version_origin, version_dest in redirects_list.copy(): 
#            normalized_origin, normalized_destination = normalize(version_origin, version_dest)
#            if normalized_origin == origin and destination == normalized_destination:
#               redirects_list.remove((version_origin, version_dest))
#               found = True
#               continue
#         if not found:
#             print("wildcard not found in version")
#             print (origin, destination)
           
#     return redirects_list

def get_file_diff(file_name_one, file_name_two):
    redirects_arr_full = pd.read_csv(file_name_one)
    if len(redirects_arr_full.columns) == 3:
        redirects_arr = redirects_arr_full.drop(redirects_arr_full.columns[2], axis = 1)
    else: 
        redirects_arr = redirects_arr_full

    redirects_one=(set([*map(tuple,redirects_arr.values)]))

    redirects_arr_full = pd.read_csv(file_name_two)
    if len(redirects_arr_full.columns) == 3:
        redirects_arr = redirects_arr_full.drop(redirects_arr_full.columns[2], axis = 1)
    else: 
        redirects_arr = redirects_arr_full
    redirects_two=(set([*map(tuple,redirects_arr.values)]))

    print(len(redirects_one), len(redirects_two))
    return redirects_one-redirects_two


def test_redirect(origin: str, destination: str) -> bool:
    resp = requests.head(origin)
    if resp.status_code != 301:
        print(f"FAIL: {origin}, status code {resp.status_code}")
        resp.close()
        return False
    elif resp.headers["Location"] != destination and resp.headers["Location"] != f"{destination}index.html":
        print(f"FAIL: {origin} -> {destination}, found {resp.headers['Location']}")
        resp.close()
        return False
    else:
        # print(f"SUCCESS: {origin} -> {destination}, found {resp.headers['Location']}")
        resp.close()
        return True
    


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
    removal_candidates = [BASE, index_suffix]
    for substring in removal_candidates:
        origin = origin.replace(substring, "")
        destination = destination.replace(substring, "")
    origin = ensure_slashes(origin)
    destination = ensure_slashes(destination)

    return origin, destination


def convert_redirect_format(source_file_name: str):
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


def parse_raw_versions(raw_versions: str):
    """
    Convert a plain text list of versions into a Python list of strings.
    
    Args:
        raw_text (str): Multiline string with one version per line.
        
    Returns:
        list[str]: List of version strings.
    """
    return [line.strip() for line in raw_versions.strip().splitlines() if line.strip()]



def ensure_starts_with_slash(path: str) -> str:
    """Ensure the path starts with a forward slash."""
    return path if path.startswith("/") else "/" + path

def ensure_ends_with_slash(path: str) -> str:
    """Ensure the path ends with a forward slash."""
    return path if path.endswith("/") else path + "/"

def ensure_slashes(path: str) -> str:
    """Ensure the path starts and ends with a forward slash."""
    path = ensure_starts_with_slash(path)
    path = ensure_ends_with_slash(path)
    return path


def create_alias_dict(raw_text: str) -> dict:
    """
    Converts a tab- or space-separated list of pairs into a dictionary,
    where the second item in each pair is the key, and the first items
    are collected in a list.

    Args:
        raw_text (str): Multiline string of pairs.

    Returns:
        dict[str, list[str]]: Dictionary with second item as key, and list of firsts as values.
    """
    result = {}
    for line in raw_text.strip().splitlines():
        if line.strip():
            first, second = line.strip().split()
            result.setdefault(second, []).append(first)
    return result

# Example usage:
raw_input = """
master    upcoming
v6.17     current
"""


def main():
    GENERATED_WILDCARDS = './generated-wildcards.csv'
    # wildcards_arr: list[list[str]] = pd.read_csv(GENERATED_WILDCARDS).values
    # redirects = get_associated_manual_version_redirects(version)
    
    # output_list = remove_wildcard_captures(wildcards_arr, redirects)
    # print (len(output_list))
    # if version in rapid_versions:
    #     version_page_levels, to_specific_manual_page = separate_page_levels(output_list, version)
    #     print(len(version_page_levels), len(to_specific_manual_page))

    #     df = pd.DataFrame(version_page_levels, columns = ['Origin', 'Redirect'])
    #     df.to_csv(f"./rapids-internal-redirects/{version}-discards.csv", index= False)
    #     df = pd.DataFrame(to_specific_manual_page, columns = ['Origin', 'Redirect'])
    #     df.to_csv(f"./wildcard-outputs/version-{version}.csv", index= False)

    # else:
    #     df = pd.DataFrame(output_list, columns = ['Origin', 'Redirect'])
    #     df.to_csv(f"./wildcard-outputs/version-{version}.csv", index= False)


if __name__ == "__main__":
    main()