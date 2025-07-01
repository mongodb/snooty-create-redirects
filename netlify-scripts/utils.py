import requests
import os
import json
import pandas as pd

rapid_versions = {'v5.1', 'v5.2', 'v5.3', 'v6.1', 'v6.2', 'v6.3', 'v7.1', 'v7.2', 'v7.3'}



## Assumes that each path has a leading slash, otherwise index from 2
def get_branch(redirect: str, index = 3):
    try:
        branch = redirect.split("/")[index]
    except IndexError as e: 
        print(redirect, branch, index)
    return branch


def add_path_placeholder(path: str, branch_index: int, replacement: str):
    branch = get_branch(path, branch_index)
    new_path = path.replace(branch, replacement)
    return new_path

## Adds ":version" in place of the branch name
def add_placeholder(redirect: tuple, branch_index: int, replacement: str):
    return add_path_placeholder(redirect[0], branch_index, replacement), add_path_placeholder(redirect[1], branch_index, replacement)

def separate_page_levels(redirects: list, version: str):
    version_page_levels = []
    to_specific_manual_page = []
    prefix = f"/docs/{version}"
    for redirect in redirects:
        if redirect[1].startswith(prefix):
            version_page_levels.append(redirect)
        else:
            to_specific_manual_page.append(redirect)
    return version_page_levels, to_specific_manual_page
    


#  takes a redirect pair in the form of a list of two strings
def replace_version(redirect: list[str], version = "v8.1"):
    return (redirect[0].replace(":version", version).strip(), redirect[1].replace(":version", version).strip())
    


def get_associated_manual_version_redirects(version: str):
    consolidated_redirect_csv = f"../netlify-redirects/netlify-docs-{version}-2-consolidated.csv"
    consolidated_redirect_2_csv = f"../netlify-redirects/netlify-docs-{version}-consolidated.csv"
    redirect_csv = f"../netlify-redirects/netlify-docs-{version}.csv"
    if os.path.exists(consolidated_redirect_2_csv) and os.path.getsize(consolidated_redirect_2_csv) != 0:
        redirects = pd.read_csv(consolidated_redirect_2_csv)

    elif os.path.exists(consolidated_redirect_csv) and os.path.getsize(consolidated_redirect_csv) != 0:
        redirects = pd.read_csv(consolidated_redirect_csv)
        
    elif os.path.exists(redirect_csv) and os.path.getsize(redirect_csv) != 0:
        redirects = pd.read_csv(redirect_csv)

    return (set([*map(tuple,redirects.values)]))


def remove_wildcard_captures(wildcard_list: list[list[str]], redirects_list: list[tuple])-> list[str]:
    wildcards= ((list([*map(replace_version,wildcard_list)])))
    
    for origin, destination in set(wildcards):
        found = False
        # if the origin is found in one of the consolidated redirects, pop that redirect from consolidated redirects
        for version_origin, version_dest in redirects_list.copy(): 
           normalized_origin, normalized_destination = normalize(version_origin, version_dest)
           if normalized_origin == origin and destination == normalized_destination:
              redirects_list.remove((version_origin, version_dest))
              found = True
              continue
        if not found:
            print("wildcard not found in version")
            print (origin, destination)
           
    return redirects_list

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

    # TODO: double check the format expected by netlify
    if not destination.startswith("/"):
        destination = "/" + destination

    if not origin.startswith("/"):
        origin = "/" + origin

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





def main():
    GENERATED_WILDCARDS = './generated-wildcards.csv'
    wildcards_arr: list[list[str]] = pd.read_csv(GENERATED_WILDCARDS).values
    redirects = get_associated_manual_version_redirects(version)
    
    output_list = remove_wildcard_captures(wildcards_arr, redirects)
    print (len(output_list))
    if version in rapid_versions:
        version_page_levels, to_specific_manual_page = separate_page_levels(output_list, version)
        print(len(version_page_levels), len(to_specific_manual_page))

        df = pd.DataFrame(version_page_levels, columns = ['Origin', 'Redirect'])
        df.to_csv(f"./rapids-internal-redirects/{version}-discards.csv", index= False)
        df = pd.DataFrame(to_specific_manual_page, columns = ['Origin', 'Redirect'])
        df.to_csv(f"./wildcard-outputs/version-{version}.csv", index= False)

    else:
        df = pd.DataFrame(output_list, columns = ['Origin', 'Redirect'])
        df.to_csv(f"./wildcard-outputs/version-{version}.csv", index= False)


if __name__ == "__main__":
    main()