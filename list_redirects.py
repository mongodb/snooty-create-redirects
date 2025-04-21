import boto3
import sys
from pathlib import Path
import json
import re

BUCKET_NAME = "docs-mongodb-org-dotcomprd"
# USAGE:
# python3 list_redirects.py <project> <versions>
# ie:
# python3 list_redirects.py atlas-cli v1.5 v1.6 v1.7
project_name = sys.argv[1]
versions = sys.argv[2:]

PROJECTS = [
    {
        "project": "ops-manager",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/ops-manager/",
    },
    {
        "project": "docs-k8s-operator",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/kubernetes-operator/",
    },
    {
        "project": "bi-connector",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/bi-connector/",
    },
    {
        "project": "kafka-connector",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/kafka-connector/",
    },
    {
        "project": "mongocli",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/mongocli/",
    },
    {
        "project": "atlas-cli",
        "bucket": "docs-atlas-dotcomprd",
        "path": "docs/atlas/cli/",
    },
    {
        "project": "node",
        "bucket": "docs-node-dotcomprd",
        "path": "docs/drivers/node/",
    },
    {
        "project": "scala",
        "bucket": "docs-languages-dotcomprd",
        "path": "docs/languages/scala/scala-driver/",
    },
    {
        "project": "spark-connector",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/spark-connector/",
    },
    {
        "project": "bi-connector",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/bi-connector/",
    },
    {
        "project": "visual-studio-extension",
        "bucket": "docs-mongodb-org-dotcomprd",
        "path": "docs/mongodb-analyzer/",
    },
    {
        "project": "csharp",
        "bucket": "docs-csharp-dotcomprd",
        "path": "docs/drivers/csharp/",
    },
]
TARGET_VERSION = "current"

def find_project(project_name):
    for project in PROJECTS:
        if project['project'] == project_name:
            return project
    return None

project = find_project(project_name) 

if not project:
    print(f'PROJECT NOT FOUND FOR {project_name}')
    sys.exit()


def remove_prefix(s: str, version: str, path:str ) -> str:
    return s.split(f"{path}{version}", 1)[-1]


s3 = boto3.resource("s3")
version_db = {}
my_bucket = s3.Bucket(project["bucket"])

for version in versions + [TARGET_VERSION]:
    output_pathname = Path(project_name +  "-" + version + ".txt")
    if output_pathname.exists():
        version_db[version] = {remove_prefix(k, version, project["path"]): v for k, v in json.loads(output_pathname.read_text())}
        continue

    files = []
    for obj in my_bucket.objects.filter(Prefix=f"{project['path']}{version}/"):
        if not obj.key.endswith("index.html"):
            continue
        redirect_target = None
        if obj.size == 0:
            real_obj = s3.Object(project['bucket'], obj.key)
            redirect_target = real_obj.website_redirect_location
        files.append([obj.key, redirect_target])
    output_pathname.write_text(json.dumps(files, indent=2))
    version_db[version] = {remove_prefix(k, version, project["path"]): v for k,v in files}

for version in versions:
    for key, _ in version_db[version].items():
        prefix_to_full_url = f"https://www.mongodb.com/{project['path']}{version}"
        fully_qualified_key = f"{prefix_to_full_url}{key}"
        if key in version_db[TARGET_VERSION]:
            redirect_target = version_db[TARGET_VERSION][key]
            if redirect_target:
                print('check redirect_target')
                print(redirect_target)
                print(f"{fully_qualified_key}\t{redirect_target}\t\tTRUE")
            else:
                key_without_indexhtml = key.removesuffix("/index.html") + "/"
                print(f"{fully_qualified_key}\thttps://www.mongodb.com/{project['path']}{TARGET_VERSION}{key_without_indexhtml}")
        else:
            print(f"{prefix_to_full_url}{key}\thttps://www.mongodb.com/{project['path']}{TARGET_VERSION}/\tTRUE")