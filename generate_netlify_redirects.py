import json
import boto3
import yaml
from consolidate_redirects import consolidate, create_s3_bucket_redirects, find_unexecutable_redirects
from scrape_s3_redirects import find_redirects, get_bucket_objects_list, writeRedirectsToFile
from sort_redirects import compile_list, sort_by_project
from typing import Dict, List

from test_redirects import test_all_redirects

def main():
    KEY_REFRESH = False
    bucket = "docs-mongodb-org-dotcomprd"
    # Subdir must be a project(ex: "docs/bi-connector") or a valid docs version
    subdir = "docs/mongoid" 
    first_index = 4080
    last_index = 4100
    s3_connection = boto3.session.Session().client("s3")

    ## Gets list of objects in the bucket (aka keys)
    keys: list[str] = get_bucket_objects_list(bucket, subdir, first_index, last_index, KEY_REFRESH)
         
    ## Get all redirects that exist on a given list of keys
    ## Returns list of tuples with each tuple of the form (<origin>, <destination>)
    redirects: list[tuple] = find_redirects(bucket, keys, s3_connection)

    # redirects_file = open(
    #  f"scraped-redirects/{bucket}-redirects.json", "r+"
    # )
    redirects_file = open(
        f"scraped-redirects/testing.json", "r+"
    )
    redirects_file_key = f"{first_index}-{last_index}" if not subdir else f"{subdir}, {first_index}-{last_index}"
    pregenerated_redirects: Dict[str, list[tuple]] = json.load(redirects_file)

    ## Add the new redirects to the list of redirects already retrieved for that bucket and write to the file 
    # each entry represents an instance in which the script has been run locally
    all_redirects:Dict[str, list[tuple]] = writeRedirectsToFile(pregenerated_redirects, redirects, f"{redirects_file_key}", redirects_file)
    
    ## Compile all of the redirects into a list of tuple-redirects
    compiled_redirects: List[tuple] = compile_list(all_redirects)
    
    ## Convert list of redirects-as-tuples into a dictionary, each key as the project of the redirect origin
    ## (except manual, which is keyed by version)
    sorted_page_redirects: Dict[str, List[tuple]] = sort_by_project(compiled_redirects)
    

    ## Get and format bucket redirects from S3 docs-mongodb-org-dotcomprd bucket
    with open("s3_buckets.json", "r") as s3_file:
        buckets = yaml.safe_load(s3_file)
        s3_bucket_redirects_list= create_s3_bucket_redirects(buckets)

    executable_redirects, unexecutable_redirects = find_unexecutable_redirects(sorted_page_redirects[subdir], s3_bucket_redirects_list)
    print(f"{len(unexecutable_redirects)} unexecutable redirects found")
    consolidation_dict =  consolidate(executable_redirects)


    successes, failures = test_all_redirects(executable_redirects)
    

if __name__ == "__main__":
    main()