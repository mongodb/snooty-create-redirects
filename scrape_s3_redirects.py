import argparse
import boto3
import datetime
import time
import json
from pathlib import Path
import botocore

object_keys_file = "scraped-redirects/keys.txt"


def extractKey(s3_object) -> list:
    return s3_object.key


#TODO: convert this to a dictionary with a list of dictionaries
#TODO: append, not overwrite, if bucket key doesn't already exist. otherwise overwrite
def write_bucket_objects_list(bucket: str ) -> list:
    print(f"Getting complete list of objects for bucket {bucket}")
    bucketObj = boto3.session.Session().resource("s3").Bucket(bucket)
    s3_objects = list(bucketObj.objects.all())
    keys = {bucket: sorted(list(map(extractKey, s3_objects)))}
    print(f"Number of objects in bucket: {len(s3_objects)}")
    f = open(object_keys_file, "w")
    f.write("\n")
    f.write(json.dumps(keys))
    return keys[bucket]

def get_bucket_objects_list( bucket: str, subdir: str, first_index: int, last_index: int, refresh= True) -> list:
    f = open(f"scraped-redirects/keys.txt")

    object_keys_list =json.load(f)
    if not refresh and object_keys_list[bucket]:
        # If there already exists the list of objects in the given bucket and we don't set refresh to true, 
        # then set the list of objects to it
        from_string = "local file scraped-redirects/keys.txt"
        objects_list = object_keys_list[bucket]
    else:
        objects_list = write_bucket_objects_list(bucket)
        from_string = "s3"
    if subdir:
        specific_keys = []
        for key in objects_list:
            if key.startswith(subdir):
                specific_keys.append(key)
        objects_list=specific_keys
    print(f"Retrieved keys for {bucket} with subdir {subdir} from {from_string}. Returning keys from index {first_index} to {last_index}")
    return objects_list[first_index:last_index]



def writeRedirectsToFile(pregenerated_redirects: dict, redirects: list, key: str, redirects_file: str) -> bool:
    try:
        if redirects:         
            pregenerated_redirects.setdefault(key, [])
            pregenerated_redirects[key] = pregenerated_redirects[key]+ redirects
        json_redirects = json.dumps(pregenerated_redirects, sort_keys=True, indent=1)
        redirects_file.seek(0)
        redirects_file.write(json_redirects)
        redirects_file.truncate()
    except json.JSONDecodeError as e: 
        print ("Error writing new redirects to file, ", e)
        return False
    return pregenerated_redirects


def find_redirects(bucket: str, keys: list, s3_connection: boto3.client)->dict:
    redirects = []

    print(f"Beginning to iterate over objects keys to find all redirect objects")
    for key in keys:
        if key.find("html")!= -1:
            try:
                response = s3_connection.head_object(Bucket=bucket, Key=key)
            except botocore.exceptions.ClientError as e:
                print(f'Error connecting to S3: {e}')
                print('Please try refreshing credentials and check the bucket argument')
                end_time = time.time()
                print(f"End time: {end_time}")
                print('Will not find new redirects, exiting "find_redirects" function...')
                return
            if response and "ResponseMetadata" in response:
                metadata = response["ResponseMetadata"]
                if "HTTPHeaders" in metadata:
                    headers = metadata["HTTPHeaders"]
                    if "x-amz-website-redirect-location" in headers:
                        redirects.append((key, headers["x-amz-website-redirect-location"]))
    print(
        f"Found {len(redirects)} redirect objects out over {len(keys)} objects iterated over in bucket {bucket}"
    )
    return redirects


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", default = "docs-mongodb-org-dotcomprd", help="Which bucket to to look in", type=str)
    parser.add_argument("--subdir", help = "Specify the subdir in which you'd like to retrieve redirects", type = str)
    parser.add_argument("--firstIndex", default = 0, help="First index for objects", type=int)
    parser.add_argument("--lastIndex", help="Last index for objects", type=int)
    parser.add_argument("--keyRefresh", default = True, help = "Whether to refresh the list of object keys for the given bucket")

    args = parser.parse_args()
    bucket = args.bucket
    key_refresh = args.keyRefresh
    subdir = args.subdir
    first_index = args.firstIndex
    last_index = len(keys) - 1 if not args.lastIndex else args.lastIndex



    start_time = time.time()
    now = datetime.datetime.now()
    print(f"Current date and time using datetime: {now}")

    keys = get_bucket_objects_list(key_refresh, bucket, subdir, first_index, last_index)

    s3_connection = boto3.session.Session().client("s3")
    redirects_file = open(
        f"scraped-redirects/{bucket}-redirects.json", "r+"
    )
    pregenerated_redirects = json.load(redirects_file)
    redirects_file.seek(0)

    redirects_file_key = f"{first_index}-{last_index}" if not subdir else f"{subdir}, {first_index}-{last_index}"

    ##TODO- should be able to do this for all of the redirects of a certain bucket(i think it does that as long as first and last index aren't specified??)
    ##TODO- should be sorted
    redirects = find_redirects(bucket, keys, s3_connection, redirects_file, redirects_file_key)
    status = writeRedirectsToFile(pregenerated_redirects, redirects, f"{redirects_file_key}", redirects_file)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()





