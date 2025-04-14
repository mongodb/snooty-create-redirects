import argparse
import json
import re
import yaml
from convert_redirects_to_netlify import normalize
from sort_redirects import sort_by_project


# captures redirects for all projects in dotcomprd 
bucket_keys= ["docs/atlas-open-service-broker", "docs/bi-connector", "docs/charts", "docs/mongodb-intellij", "docs/drivers/php/laravel-mongodb", "docs/cloud-manager", "docs/ops-manager", "docs/kubernetes-operator","docs/mongodb-analyzer", "docs/spark-connector", "docs/drivers/rust","docs/ruby-driver", "docs/php-library", "docs/mongoid", "docs/mongodb-vscode", "docs/mongodb-shell", "docs/mongocli","docs/meta","docs/drivers/kotlin/coroutine", "docs/kafka-connector", "docs/guides", "docs/entity-framework", "docs/drivers", "docs/relational-migrator", "docs/404", "docs/datalake", "docs/database-tools", "docs/csfle-merge"
"docs/compass", "docs/cluster-to-cluster-sync"]
#  captures rediredcts for  non-numerical docs branches
docs_branches = ["docs/core", "docs/master", "docs/current", "docs/rapid", "docs/upcoming", "docs/manual", "docs-stable"]


# def create_bucket_redirects_list(routing_rules: list) -> set:
#     bucket_redirects_list = set()
#     for item in routing_rules:
#         key_prefix = item["RoutingRuleCondition"]["KeyPrefixEquals"]
#         origin = key_prefix.split("/", 1)[1]
#         # prefix = find first slash and divide string there
#         replacement = item["RedirectRule"]["ReplaceKeyPrefixWith"]
#         destination = replacement.split("/", 1)[1]
#         redirect_obj = (origin, destination)
#         bucket_redirects_list.add(redirect_obj)

#     return bucket_redirects_list

def create_s3_bucket_redirects(routing_rules: list)-> list[tuple]:
    s3_bucket_redirects_list = []
    for item in routing_rules:
        origin = item["Condition"]["KeyPrefixEquals"]
        destination = item["Redirect"]["ReplaceKeyPrefixWith"]
        redirect_obj = (origin, destination)
        s3_bucket_redirects_list.append(redirect_obj)

    return s3_bucket_redirects_list

                

def find_unexecutable_redirects(project_redirect_section, s3_bucket_redirects_list: list[tuple])-> tuple:
        unexecutable_redirects: dict = {}
        executable_redirects = set()
     # iterate over a list containing lists of page-level redirects
     # for project_redirect_section in sorted_page_redirects.values():
        for page_level_redirect in project_redirect_section:
            origin = list(page_level_redirect)[0]
            executable = True
            for bucket_redirect in s3_bucket_redirects_list:
                # If one of the bucket level redirects is present in the origin url, the redirect will never execute
                # POSSIBLY ADD SLASH HERE, test "docs/compass/v1 xxx- " REDIRECT THING, does the redirect capture it??
                # change this to dictionary of list of tuples with key as reason, list of redirect tuples as value
                if origin.find(bucket_redirect[0]) != -1:
                    unexecutable_redirects.setdefault(bucket_redirect, [])
                    print(page_level_redirect)
                    unexecutable_redirects[bucket_redirect] = unexecutable_redirects[bucket_redirect]+ [page_level_redirect]
                    executable = False
            if executable:
                executable_redirects.add(page_level_redirect)
        return (executable_redirects, unexecutable_redirects)


def isValidProject(project: str)-> bool:
    # TODO: add conditional for just being under "docs" (docs-landing) 
    return bool(re.search("^(docs\/v)\d\.\d$", project)) or project in docs_branches or project in bucket_keys

def consolidate(redirects: list):
    # Takes a list of dictionaries of length 2
    # Takes a list of tuples
    potential_bucket_keys= {}
    for redirect in redirects:
        origin, destination = normalize(redirect[0], redirect[1])
        for i in range(1, (min(len(origin.split("/")), len(destination.split("/"))))):
            origin_branch = origin.split("/")[i]
            destination_branch = destination.split("/")[i]
            if origin.replace(origin_branch, destination_branch) == destination:
                val = potential_bucket_keys.setdefault((origin_branch, destination_branch), 0 )
                potential_bucket_keys[(origin_branch, destination_branch)] = val+1

    bucket_keys = {pair: count for pair, count in potential_bucket_keys.items() if not count==1}

    
    if len(bucket_keys):
        print(f"Consolidatable redirect paths: {bucket_keys}")
    return bucket_keys

def writeSortedRedirectsToFiles(sorted_page_redirects, s3_bucket_redirects_list)-> None:
    unexecutable_redirects_list=[]
    extraneous_redirects = json.load(open(
        f"scraped-redirects/sorted/extraneous-redirects.json", "a"
    ))
    extraneous_redirects.seek(0)
    for project in sorted_page_redirects.keys():
        valid_project_name = isValidProject(project)
        if valid_project_name:
            executable_redirects, unexecutable_redirects = find_unexecutable_redirects(sorted_page_redirects[project], s3_bucket_redirects_list)
            consolidate(executable_redirects)
            print(len(unexecutable_redirects), len(executable_redirects), project)
            fileName = project.replace("/", "-")
            with open(f"scraped-redirects/sorted/{fileName}.json", "w") as file:
                file.write(json.dumps(sorted(executable_redirects)))
            if len(unexecutable_redirects):
                unexecutable_redirects_list.append(unexecutable_redirects)
        else:
            # add conditional so that only writes to own file for projects that have a project name that makes sense. otherwise just write to a big dump file
            with open(f"scraped-redirects/sorted/extraneous-redirects.json", "a") as file:
                #convert extraneous redirects to set if want to do only for a specific project and then add new redirects to script then write to file
                file.write(json.dumps(sorted_page_redirects[project]))
        if len(unexecutable_redirects):
            with open(f"scraped-redirects/unexecutable/{fileName}.json", "w") as unexecutable_file:
                unexecutable_file.write(json.dumps(unexecutable_redirects))
    return
        



#TODO
def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default= "docs-mongodb-org-dotcomprd", help="Which bucket to to look in", type=str)
    args = parser.parse_args()
    bucket = args.bucket

    # Get and format bucket redirects from buckets.yml from the autobuilder
    # with open("buckets.yml", "r") as f:
    #     buckets = yaml.safe_load(f)
    #     routing_rules = buckets["Resources"]["DocsBucket"]["Properties"][
    #         "WebsiteConfiguration"
    #     ]["RoutingRules"]
    # bucket_redirects_list = create_bucket_redirects_list(routing_rules)
    # print(len(bucket_redirects_list))

    # Get and format bucket redirects from S3 docs-mongodb-org-dotcomprd bucket
    with open("s3_buckets.json", "r") as s3_file:
        buckets = yaml.safe_load(s3_file)
        s3_bucket_redirects_list= create_s3_bucket_redirects(buckets)

    redirects = json.load(open(f"scraped-redirects/{bucket}-redirects.json", "r"))

    # Sort object redirects by project
    sorted_page_redirects = sort_by_project(redirects)


    writeSortedRedirectsToFiles(sorted_page_redirects, s3_bucket_redirects_list)






if __name__ == "__main__":
    main()






#group redirects by bucket
#separate out "bad" or unecessary redirects (ones like "docs/current//reference/command/dbStats/index.html": "https://www.mongodb.com/docs/current/reference/command/dbStats")
#find redirects captured by bucket-level redirects
    # check if we should add slash to end of bucket level redirect origin comparing to
    # verify assumptions manually

#remove above categories of redirects
#see which we can consolidate- offline docs project, just make those into wildcard redirects pls?? (see if beginning path matches end path with substitution)