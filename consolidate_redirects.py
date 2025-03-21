import argparse
import json
import re
import yaml


# captures redirects for all projects in dotcomprd
bucket_keys= ["docs/atlas-open-service-broker", "docs/bi-connector", "docs/charts", "docs/mongodb-intellij", "docs/drivers/php/laravel-mongodb", "docs/cloud-manager", "docs/ops-manager", "docs/kubernetes-operator","docs/mongodb-analyzer", "docs/spark-connector", "docs/drivers/rust","docs/ruby-driver", "docs/php-library", "docs/mongoid", "docs/mongodb-vscode", "docs/mongodb-shell", "docs/mongocli","docs/meta","docs/drivers/kotlin/coroutine", "docs/kafka-connector", "docs/guides", "docs/entity-framework", "docs/drivers", "docs/relational-migrator", "docs/404", "docs/datalake", "docs/database-tools", "docs/csfle-merge"
"docs/compass", "docs/cluster-to-cluster-sync"]

def create_bucket_redirects_list(routing_rules: list) -> set:
    bucket_redirects_list = set()
    for item in routing_rules:
        key_prefix = item["RoutingRuleCondition"]["KeyPrefixEquals"]
        origin = key_prefix.split("/", 1)[1]
        # prefix = find first slash and divide string there
        replacement = item["RedirectRule"]["ReplaceKeyPrefixWith"]
        destination = replacement.split("/", 1)[1]
        redirect_obj = (origin, destination)
        bucket_redirects_list.add(redirect_obj)

        # print(f" {origin} --> {destination} \n")
    return bucket_redirects_list

def create_s3_bucket_redirects(routing_rules: list)-> list:
    s3_bucket_redirects_list = []
    for item in routing_rules:
        origin = item["Condition"]["KeyPrefixEquals"]
        destination = item["Redirect"]["ReplaceKeyPrefixWith"]
        redirect_obj = (origin, destination)
        s3_bucket_redirects_list.append(redirect_obj)

    return s3_bucket_redirects_list


def sort_by_project(redirects: list)-> dict:
    redirects_buckets={}
    for redirect_chunk in redirects:
        for origin, destination in list(redirect_chunk.values())[0].items():
            if origin.find("docs/")==0:
                project=origin[5:].split("/")[0]
                redirects_buckets.setdefault("docs/"+project, []).append({origin:destination})
        
            # else:
            #     print("bad redirect:", origin, destination)
    return redirects_buckets
                

def find_unexecutable_redirects(project_redirect_section: dict, s3_bucket_redirects_list: list)->list:
        unexecutable_redirects= []
        executable_redirects =[]
     # iterate over a list containing lists of page-level redirects
    # for project_redirect_section in sorted_page_redirects.values():
    
        for page_level_redirect in project_redirect_section:
            origin = list(page_level_redirect)[0]
            # print(origin)
            executable = True
            for bucket_redirect in s3_bucket_redirects_list:
                # If one of the bucket level redirects is present in the origin url, the redirect will never execute
                #  POSSIBLY ADD SLASH HERE, test "docs/compass/v1 xxx- " REDIRECT THING, does the redirect capture it??
                if origin.find(bucket_redirect[0]) != -1:
                    unexecutable_redirects.append({'redirect': page_level_redirect, 'reason': bucket_redirect})
                    executable = False
            if executable:
                executable_redirects.append({"origin":list(page_level_redirect.keys())[0], "destination":list(page_level_redirect.values())[0]})
        return executable_redirects, unexecutable_redirects


def isValidProject(project: str)-> bool:
    # add conditional for just being under "docs" (docs-landing) and for manuals branches that aren't numerical
    return bool(re.search("^(docs\/v)\d\.\d$", project)) or project in bucket_keys

def writeSortedRedirectsToFiles(sorted_page_redirects, s3_bucket_redirects_list):
    for project in sorted_page_redirects.keys():
        valid_project_name = isValidProject(project)
        if valid_project_name:
            fileName = project.replace("/", "-")
            with open(f"scraped-redirects/sorted/{fileName}.json", "w") as file:
                executable_redirects, unexecutable_redirects = find_unexecutable_redirects(sorted_page_redirects[project], s3_bucket_redirects_list)
                print(len(unexecutable_redirects), len(executable_redirects), project)
                file.write(json.dumps(sorted(executable_redirects, key= lambda k: k["origin"])))
        else:
            # add conditional so that only writes to own file for projects that have a project name that makes sense. otherwise just write to a big dump file
            with open(f"scraped-redirects/sorted/extraneous-redirects.json", "a") as file:
                file.write(json.dumps(sorted_page_redirects[project]))
        



def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Which bucket to to look in", type=str)
    args = parser.parse_args()
    bucket = args.bucket if args.bucket else "docs-mongodb-org-dotcomprd"

    with open("buckets.yml", "r") as f:
        buckets = yaml.safe_load(f)
        routing_rules = buckets["Resources"]["DocsBucket"]["Properties"][
            "WebsiteConfiguration"
        ]["RoutingRules"]
    bucket_redirects_list = create_bucket_redirects_list(routing_rules)

    print(len(bucket_redirects_list))


    with open("s3_buckets.json", "r") as s3_file:
        buckets = yaml.safe_load(s3_file)
        s3_bucket_redirects_list= create_s3_bucket_redirects(buckets)

    redirects = json.load(open(f"scraped-redirects/{bucket}-redirects.json", "r"))

        # sort page-level redirects by project
    sorted_page_redirects = sort_by_project(redirects)
    # print(sorted_page_redirects.keys())
    writeSortedRedirectsToFiles(sorted_page_redirects, s3_bucket_redirects_list)


    # Find redirects that are unexecutable because a bucket-level redirect executed first
    # unexecutable_redirects= find_unexecutable_redirects(sorted_page_redirects, s3_bucket_redirects_list)
    # print(unexecutable_redirects)
    # print("Number of unexecutable redirects:", len(unexecutable_redirects))







if __name__ == "__main__":
    main()






#group redirects by bucket
#separate out "bad" or unecessary redirects (ones like "docs/current//reference/command/dbStats/index.html": "https://www.mongodb.com/docs/current/reference/command/dbStats")
#find redirects captured by bucket-level redirects
    # check if we should add slash to end of bucket level redirect origin comparing to
    # verify assumptions manually

#remove above categories of redirects
#see which we can consolidate- offline docs project, just make those into wildcard redirects pls?? (see if beginning path matches end path with substitution)