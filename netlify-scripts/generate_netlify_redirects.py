import json
import os
import boto3
import yaml
from typing import Dict, List
from scrape_s3_redirects import (
    find_redirects,
    get_bucket_objects_list,
    writeRedirectsToFile,
)
import pandas as pd
from sort_redirects import compile_list, sort_by_project
from consolidate_redirects import (
    consolidate,
    create_s3_bucket_redirects,
    find_unexecutable_redirects,
)
from test_redirects import test_all_redirects
from utils import normalize


def write_to_csv(redirects: list[tuple], output_file_name: str) -> list[str]:
    DESTINATION_FILE_CSV = f"../netlify-redirects/{output_file_name}.csv"
    csv_output_rules = []
    print("writing to csv")
    print(len(redirects))
    for redirect in redirects:
        origin, destination = normalize(redirect[0], redirect[1])
        csv_output_rules.append((origin, destination))
    df = pd.DataFrame(csv_output_rules, columns = ['origin', 'destination'])
    df.to_csv(DESTINATION_FILE_CSV, index= False)

def convert(redirects: list[tuple], output_file_name: str) -> list[str]:
    """Converts redirect tuples to Netlify format

    Args:
    redirects: list of redirect pairs as tuples

    Returns:
    The reformatted redirects as a list of strings
    """
    output_rules = []  
    for redirect in redirects:
        ##TODO: change this to a different normalize ? (remove "docs")
        origin, destination = normalize(redirect[0], redirect[1])
        ## Add a comment for what the original redirect was?
        output_rules.append(
            '\n[[redirects]] \rfrom = "' + origin + '"\rto = "' + destination + '"\r\r'
        )


    DESTINATION_FILE = f"../netlify-redirects/{output_file_name}.txt"
    f = open(DESTINATION_FILE, "w+")
    f.write("".join(output_rules))
    return output_rules


# TODO: add parser args
def main():
    KEY_REFRESH = True
    bucket = "docs-mongodb-org-dotcomprd"
    # Subdir must be a project(ex: "docs/bi-connector") or a valid docs version
    subdir = "docs/cloud-manager"
    output_file_name = "netlify-cloud-manager-2"
    # 168197
    # last_index = 168197
    last_index = 500000
    first_index = 0
    # leave empty if unversioned
    online_branches = []
    s3_connection = boto3.session.Session().client("s3")


    ## Gets list of objects in the bucket (aka keys)
    keys: list[str] = get_bucket_objects_list(
        bucket, subdir, first_index, last_index, KEY_REFRESH
    )

    ## Get all redirects that exist on a given list of keys
    ## Returns list of tuples with each tuple of the form (<origin>, <destination>)
    redirects: list[tuple] = find_redirects(bucket, keys, s3_connection)
    # print(f"set of redirects found has length {len(redirects)}")
    if len(redirects)== 0:
        print("No redirects found, returning")
        return
    
    redirects_file_key: str = (
        f"{first_index}-{last_index}"
        if not subdir
        else f"{subdir}, {first_index}-{last_index}"
    )


    # redirects_file = open(f"resources/scraped-redirects/{bucket}-redirects.json", "r+")
    # if os.path.exists(f"resources/scraped-redirects/{bucket}-redirects.json") and os.path.getsize(f"resources/scraped-redirects/{bucket}-redirects.json") != 0:
    #     pregenerated_redirects = json.load(redirects_file)
    # else: 
    #     redirects_file = open(f"resources/scraped-redirects/{bucket}-redirects.json", "w+")
    #     pregenerated_redirects = {}


    # ## Add the new redirects to the list of redirects already retrieved for that bucket and write to the file
    # # each entry represents an instance in which the script has been run locally
    # all_redirects: Dict[str, list[tuple]] = writeRedirectsToFile(
    #     pregenerated_redirects, redirects, f"{redirects_file_key}", redirects_file
    # )

    ## Compile all of the redirects into a list of tuple-redirects
    # compiled_redirects: List[tuple] = compile_list(all_redirects)
    ## Convert list of redirects-as-tuples into a dictionary, each key as the project of the redirect origin
    ## (Except manual, which is keyed by version)
    # sorted_page_redirects: Dict[str, List[tuple]] = sort_by_project(compiled_redirects)
    # print(sorted_page_redirects.keys())
    ## Get and format bucket redirects from S3 docs-mongodb-org-dotcomprd bucket
    with open("resources/s3_buckets.json", "r") as s3_file:
        buckets = yaml.safe_load(s3_file)
        s3_bucket_redirects_list = create_s3_bucket_redirects(buckets)
    # executable_redirects, unexecutable_redirects = find_unexecutable_redirects(
    #     sorted_page_redirects[subdir], s3_bucket_redirects_list
    # )
    executable_redirects, unexecutable_redirects = find_unexecutable_redirects(
        redirects, s3_bucket_redirects_list
    )
    print(f"{len(unexecutable_redirects)} unexecutable redirects found")
    bucket_keys, manual_wildcard_redirects, consolidated_redirects = consolidate(executable_redirects, online_branches)

    successes, failures = test_all_redirects(executable_redirects)
    convert(executable_redirects, output_file_name)

    # write_to_csv(consolidated_redirects, output_file_name+"-consolidated")
    write_to_csv(consolidated_redirects, output_file_name+"-redirects")


if __name__ == "__main__":
    main()