import argparse
import boto3
import datetime
import time
import json
from pathlib import Path
import botocore


def extractKey(s3_object) -> list:
    return s3_object.key


def writeObjectKeys(bucket) -> list:
    bucketObj = boto3.session.Session().resource("s3").Bucket(bucket)
    s3_objects = list(bucketObj.objects.all())
    keys = {bucket: list(map(extractKey, s3_objects))}
    print(f"Number of objects in bucket: {len(s3_objects)}")
    f = open(f"scraped-redirects/keys.txt", "a")
    f.write("\n")
    f.write(json.dumps(keys))
    return keys[bucket]

def getBucketKeys(file, bucket) -> list:
    if file.exists():
        f = open(f"scraped-redirects/keys.txt")
        keys_file = json.load(f)
        if keys_file[bucket]:
            keys = keys_file[bucket]
        else:
            keys = writeObjectKeys(bucket)
    else:
        keys = writeObjectKeys(bucket)
    return keys



def writeRedirectsToFile(pregenerated_redirects, redirects, key, redirects_file) -> bool:
    try:
        pregenerated_redirects.append({key: redirects})
        json_redirects = json.dumps(pregenerated_redirects, sort_keys=True, indent=1)
        redirects_file.write(json_redirects)
        redirects_file.truncate()
    except json.JSONDecodeError as e: 
        print ("Error writing new redirects to file, ", e)
        return False
    return True


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help="Which bucket to to look in", type=str)
    parser.add_argument("--firstIndex", help="First index for objects", type=int)
    parser.add_argument("--lastIndex", help="Last index for objects", type=int)
    parser.add_argument(
        "--redirectFileNumber",
        help="Skip generation of a list of object keys",
        type=bool,
    )

    args = parser.parse_args()
    bucket = args.bucket

    start_time = time.time()
    now = datetime.datetime.now()
    print(f"Current date and time using datetime: {now}")

    file = Path("scraped-redirects/keys.txt")
    keys = getBucketKeys(file, bucket)
    

    s3_connection = boto3.session.Session().client("s3")
    redirect_file_num = "-{args.redirectFileNumber}" if args.redirectFileNumber else ""
    redirects_file = open(
        f"scraped-redirects/{bucket}-redirects{redirect_file_num}.json", "r+"
    )
    pregenerated_redirects = json.load(redirects_file)
    redirects_file.seek(0)

    start_index = 0 if not args.firstIndex else args.firstIndex
    last_index = len(keys) - 1 if not args.lastIndex else args.lastIndex
    redirects = {}

    print(f"Beginning to iterate over objects in bucket {bucket} length {len(keys)} ", now)
    for key in keys[start_index:last_index]:
        if key.find("html")!= -1:
            try:
                response = s3_connection.head_object(Bucket=bucket, Key=key)
            except botocore.exceptions.ClientError as e:
                print(f'Error connecting to S3: {e}')
                print('Please try refreshing credentials and check the bucket argument')
                status = writeRedirectsToFile(pregenerated_redirects, redirects, f"{start_index}-{last_index}", redirects_file)
                end_time = time.time()
                elapsed_time = end_time - start_time
                print(f"Elapsed time: {elapsed_time} seconds")
                print('Exiting...')
                return
            if response and "ResponseMetadata" in response:
                metadata = response["ResponseMetadata"]
                if "HTTPHeaders" in metadata:
                    headers = metadata["HTTPHeaders"]
                    if "x-amz-website-redirect-location" in headers:
                        redirects[key] = headers["x-amz-website-redirect-location"]

    print(
        f"Found {len(redirects)} redirect objects out of {last_index-start_index} objects iterated over in bucket {bucket}"
    )

    status = writeRedirectsToFile(pregenerated_redirects, redirects, f"{start_index}-{last_index}", redirects_file)
    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")


if __name__ == "__main__":
    main()





