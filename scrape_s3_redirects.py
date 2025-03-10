import argparse
import boto3
import datetime
import time
import json



def writeObjectKeys(bucket)-> list:
    bucketObj = boto3.session.Session().resource("s3").Bucket(bucket)
    objects = list(bucketObj.objects.all())
    print(type(objects))
    print(f"Number of objects in bucket: {len(objects)}")
    f = open(f"scraped-redirects/{bucket}-keys.txt", 'a')
    f.write(str(objects))
    return objects

# def getObjectKeys(bucket)-> list:
#     f = open(f"scraped-redirects/{bucket}-keys.txt", 'r')
#     objects = f.read()[1:-1].split("), ")
#     print(objects[0:5])
#     return objects

def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", help = "Which bucket to to look in", type = str)
    # parser.add_argument("--skipKeyGeneration", help = "Skip generation of a list of object keys. Should be used when the script has already been run at least once for the specified bucket", type = bool)
    parser.add_argument("--firstIndex", help = "First index for objects", type = int)
    parser.add_argument("--lastIndex", help = "Last index for objects", type = int)
    parser.add_argument("--redirectFileNumber", help = "Skip generation of a list of object keys", type = bool)
  

    args = parser.parse_args()
    bucket = args.bucket

    start_time = time.time()
    now = datetime.datetime.now()
    print(f"Current date and time using datetime: {now}")



    objects = writeObjectKeys(bucket) 


    s3_connection = boto3.session.Session().client("s3")
    redirect_file_num = '-{args.redirectFileNumber}' if args.redirectFileNumber else ""
    f = open(f"scraped-redirects/{args.bucket}-redirects{redirect_file_num}.txt", 'a')
    start_index = 0 if not args.firstIndex else args.firstIndex
    last_index = len(objects)-1 if not args.lastIndex else args.lastIndex
    redirects = {}


    print(f"Beginning to iterate over objects in bucket {args.bucket} ", now)
    for object_summary in objects[start_index:last_index]:
        try:
            response= s3_connection.head_object(Bucket=args.bucket, Key=object_summary.key)
        finally:
            if response and 'ResponseMetadata' in response:
                metadata = response['ResponseMetadata']
                if 'HTTPHeaders' in metadata:
                    headers = metadata['HTTPHeaders']
                    if 'x-amz-website-redirect-location' in headers:
                        redirects[object_summary.key]= headers['x-amz-website-redirect-location']

    print(f"Found {len(redirects)} redirect objects out of {last_index-start_index} objects iterated over in bucket {bucket}")
    f.write(json.dumps(redirects, sort_keys=True))

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Elapsed time: {elapsed_time} seconds")
   

if __name__ == "__main__":
    main()


