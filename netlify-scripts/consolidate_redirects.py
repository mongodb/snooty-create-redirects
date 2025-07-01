import argparse
import json
import re
import yaml
from utils import normalize
from sort_redirects import sort_by_project
from typing import List, Tuple, Dict, Set


# List of all projects in dotcomprd
bucket_keys = [
    "docs/atlas-open-service-broker",
    "docs/bi-connector",
    "docs/charts",
    "docs/mongodb-intellij",
    "docs/drivers/php/laravel-mongodb",
    "docs/cloud-manager",
    "docs/ops-manager",
    "docs/kubernetes-operator",
    "docs/mongodb-analyzer",
    "docs/spark-connector",
    "docs/drivers/rust",
    "docs/ruby-driver",
    "docs/php-library",
    "docs/mongoid",
    "docs/mongodb-vscode",
    "docs/mongodb-shell",
    "docs/mongocli",
    "docs/meta",
    "docs/drivers/kotlin/coroutine",
    "docs/kafka-connector",
    "docs/guides",
    "docs/entity-framework",
    "docs/drivers",
    "docs/relational-migrator",
    "docs/404",
    "docs/datalake",
    "docs/database-tools",
    "docs/csfle-merge" "docs/compass",
    "docs/cluster-to-cluster-sync",
]
# List of non-numerical docs branches
docs_branches = [
    "docs/core",
    "docs/master",
    "docs/current",
    "docs/rapid",
    "docs/upcoming",
    "docs/manual",
    "docs-stable",
]



def create_s3_bucket_redirects(routing_rules: list) -> list[tuple]:
    s3_bucket_redirects_list = []
    for item in routing_rules:
        origin = item["Condition"]["KeyPrefixEquals"]
        destination = item["Redirect"]["ReplaceKeyPrefixWith"]
        redirect_obj = (origin, destination)
        s3_bucket_redirects_list.append(redirect_obj)

    return s3_bucket_redirects_list

## Args: takes in list of redirects as list of tuples, list of tuples to compare against.
def find_unexecutable_redirects(
    project_redirects: list[tuple], s3_bucket_redirects_list: list[tuple]
) -> Tuple[Set[tuple], Dict[tuple, list[tuple]]]:
    executable_redirects = set()
    unexecutable_redirects: dict = {}

    for page_level_redirect in project_redirects:
        origin = list(page_level_redirect)[0]
        executable = True
        for bucket_redirect in s3_bucket_redirects_list:
            # If one of the bucket level redirects is present in the origin url, the redirect will never execute
            if origin.find(bucket_redirect[0]) != -1:
                unexecutable_redirects.setdefault(bucket_redirect, [])
                unexecutable_redirects[bucket_redirect] = unexecutable_redirects[
                    bucket_redirect
                ] + [page_level_redirect]
                executable = False
        if executable:
            executable_redirects.add(page_level_redirect)
    return (executable_redirects, unexecutable_redirects)


def isValidProject(project: str) -> bool:
    # TODO: add conditional for just being under "docs" (docs-landing)
    return (
        bool(re.search("^(docs\/v)\d\.\d$", project))
        or project in docs_branches
        or project in bucket_keys
    )


### Consolidates redirects based on if two criteria
#1. Redirect would be "caught" by a bucket level redirect and therefore won't execute
#2. Redirect is for a branch that is already downloadable
## Accepts a list of redirects as an argument
## Returns list of potential consolidation paths, invalid branches, and the list of consolidated redirects
def consolidate(redirects: list[tuple], online_branches: list) :
    potential_bucket_keys = {}
    invalid_branch_list = []
    consolidated_redirects = set()
    for redirect in redirects:
        origin, destination = normalize(redirect[0], redirect[1])
        origin_branch =origin.split("/")[3] 
        if len(online_branches) > 0 and not origin_branch in online_branches:
                invalid_branch_list.append(redirect)
        else:       
            for i in range(1, (min(len(origin.split("/")), len(destination.split("/"))))):
                origin_branch = origin.split("/")[i]
                destination_branch = destination.split("/")[i]
                if origin.replace(origin_branch, destination_branch) == destination:
                    val = potential_bucket_keys.setdefault(
                        (origin_branch, destination_branch), 0
                    )
                    potential_bucket_keys[(origin_branch, destination_branch)] = val + 1
            

    consolidated_redirects =  set(redirects) - set(invalid_branch_list)
    print(f"length of consolidated redirects {len(consolidated_redirects)}")
    print(f"length of manual captured redirects {len(invalid_branch_list)}")

# change this to have a list of the redirects we're taking out
    bucket_keys = {
        pair: count for pair, count in potential_bucket_keys.items() if not count == 1
    }

    if len(bucket_keys):
        print(f"Consolidatable redirect paths: {bucket_keys}")
    return bucket_keys, invalid_branch_list, consolidated_redirects


def writeSortedRedirectsToFiles(
    sorted_page_redirects: Dict[str, List[tuple]], s3_bucket_redirects_list: list
) -> None:
    unexecutable_redirects_list = []
    extraneous_redirects = json.load(
        open(f"scraped-redirects/sorted/extraneous-redirects.json", "a")
    )
    extraneous_redirects.seek(0)
    for project in sorted_page_redirects.keys():
        valid_project_name = isValidProject(project)
        if valid_project_name:
            executable_redirects, unexecutable_redirects = find_unexecutable_redirects(
                sorted_page_redirects[project], s3_bucket_redirects_list
            )
            consolidate(executable_redirects)
            print(len(unexecutable_redirects), len(executable_redirects), project)
            fileName = project.replace("/", "-")
            with open(f"scraped-redirects/sorted/{fileName}.json", "w") as file:
                file.write(json.dumps(sorted(executable_redirects)))
            if len(unexecutable_redirects):
                unexecutable_redirects_list.append(unexecutable_redirects)
        else:
            # TODO: add check so that only writes to file for projects that have a project name that makes sense
            with open(
                f"scraped-redirects/sorted/extraneous-redirects.json", "a"
            ) as file:
                file.write(json.dumps(sorted_page_redirects[project]))
        if len(unexecutable_redirects):
            with open(
                f"scraped-redirects/unexecutable/{fileName}.json", "w"
            ) as unexecutable_file:
                unexecutable_file.write(json.dumps(unexecutable_redirects))
    return


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket",
        default="docs-mongodb-org-dotcomprd",
        help="Which bucket to to look in",
        type=str,
    )
    args = parser.parse_args()
    bucket = args.bucket

    # Get and format bucket redirects from S3 docs-mongodb-org-dotcomprd bucket
    with open("s3_buckets.json", "r") as s3_file:
        buckets = yaml.safe_load(s3_file)
        s3_bucket_redirects_list = create_s3_bucket_redirects(buckets)

    redirects: str = json.load(open(f"scraped-redirects/{bucket}-redirects.json", "r"))

    # Sort object redirects by project
    sorted_page_redirects: Dict[str, List[tuple]] = sort_by_project(redirects)

    executable_redirects, unexecutable_redirects = find_unexecutable_redirects(
        sorted_page_redirects[subdir], s3_bucket_redirects_list
    )
    print(f"{len(unexecutable_redirects)} unexecutable redirects found")
    consolidation_dict = consolidate(executable_redirects)

    writeSortedRedirectsToFiles(sorted_page_redirects, s3_bucket_redirects_list)


if __name__ == "__main__":
    main()