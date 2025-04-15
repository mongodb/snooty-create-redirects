import argparse
import json
from os import path
from typing import List, Tuple, Dict


def compile_list(redirects: Dict[str, list[tuple]]) -> List[tuple]:
    compiled_redirects = []
    for redirect_chunk in list(redirects.values()):
        for origin, destination in redirect_chunk:
            compiled_redirects.append((origin, destination))
    return compiled_redirects


##  Input:
#       redirects: list of dictionaries where key is indices of s3 object keys, values are dictionaries of redirects
#  [{ 0-100: {
#       "docs/drivers/php/laravel-mongodb/v4.3/sessions/index.html": "https://www.mongodb.com/docs/drivers/php/laravel-mongodb/v4.3/",
#       "docs/drivers/php/laravel-mongodb/v4.3/vector-search/index.html": "https://www.mongodb.com/docs/drivers/php/laravel-mongodb/v4.3/",
#       "docs/drivers/php/laravel-mongodb/v4.4/atlas-search/index.html": "https://www.mongodb.com/docs/drivers/php/laravel-mongodb/v4.4/",
#     }},
#     { 100-200: {
#         "docs/drivers/php/laravel-mongodb/v4.3/retrieve/index.html": "https://www.mongodb.com/docs/drivers/php/laravel-mongodb/v4.3/fundamentals/read-operations/",
#        "docs/drivers/php/laravel-mongodb/v4.3/scout/index.html": "https://www.mongodb.com/docs/drivers/php/laravel-mongodb/v4.3/",
#     }}]
#       project: The project name we'd like to get all redirects for. If no project name is specified, defaults to "all" so that all redirects are sorted into projects

##  Output: dictionary with "docs/<project_name>" as key and list of dictionaries, which are the redirects associated with that project as value
#   { docs/guides:
#       ["docs/charts/administration/backup-and-restore-keys/index.html":"https://www.mongodb.com/docs/charts/", "docs/charts/administration/configure-https-deployment/index.html", "https://www.mongodb.com/docs/charts/"]
#     docs/mongoid:
#       ["docs/mongoid/5.4/index.html", "https://www.mongodb.com/docs/mongoid/current/"]
#   }


# Accepts list of tuples
def sort_by_project(redirects: list) -> Dict[str, List[tuple]]:
    redirects_by_bucket: Dict[str, List[tuple]] = {}
    for origin, destination in redirects:
        if origin.find("docs/") == 0:
            project_name = ("/").join(origin.split("/")[0:2])
            redirects_by_bucket.setdefault(project_name, set()).add(
                (origin, destination)
            )
        else:
            # check if html file, if so is possibly a docs-landing redirect
            redirects_by_bucket.setdefault("bad_redirect", set()).add(
                (origin, destination)
            )
            print("Bad redirect:", origin, destination)
    # Sorts dictionary by key
    redirects_by_bucket = dict(
        sorted(redirects_by_bucket.items(), key=lambda item: item[0])
    )
    return redirects_by_bucket


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--bucket",
        default="docs-mongodb-org-dotcomprd",
        help="Which bucket to to look in",
        type=str,
    )
    parser.add_argument(
        "--write", default=True, help="Whether to write the output to a file", type=bool
    )

    args = parser.parse_args()
    bucket = args.bucket
    write_to_file = args.write

    redirects = json.load(open(f"scraped-redirects/{bucket}-redirects.json", "r"))

    ## Compile all of the redirects into a list of tuple-redirects
    compiled_redirects = compile_list(redirects)

    ## Convert list of redirects-as-tuples into a dictionary, each key as the project of the redirect origin
    ## (except manual, which is keyed by version)
    sorted_page_redirects: Dict[str, List[tuple]] = sort_by_project(compiled_redirects)

    # Write each redirect to its own file
    if write_to_file:
        for project, redirects in sorted_page_redirects.items():
            fileName = project.replace("/", "-")
            with open(f"scraped-redirects/sorted/{fileName}.json", "w") as file:
                file.write(json.dumps(list(redirects)))


if __name__ == "__main__":
    main()
