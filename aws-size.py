#!/usr/bin/env python3

import pymysql
import os
import boto3
from collections import defaultdict
    
def listS3bucket(bucket):
    s3 = boto3.resource("s3")
    bucket = s3.Bucket(bucket)
    for obj in bucket.objects.all():
        print(obj.key)

def copytoS3(pathfile, bucket):
    s3 = boto3.resource('s3')
    bucketdir = s3.Bucket(bucket)
    bucketdir.upload_file(pathfile, "wltest/"+os.path.split(pathfile)[-1])
    return None

def listobj(bucket, maxkeys=123, start=None):
    client = boto3.client("s3")
    response = client.list_objects_v2(Bucket=bucket, MaxKeys=maxkeys, StartAfter=start)
    print(response["MaxKeys"])
    print(response["IsTruncated"])
    for n in response["Contents"]:
        if not n['Key'].endswith("/") and not n['Key'].endswith("$folder$") and not n['Key'].endswith("_SUCCESS"):
            print(f"{n['Key']}  {n['Size']/1024/1024:.1f}")

def summary(bucket, key):
    s3 = boto3.resource("s3")
    obj = s3.ObjectSummary(bucket, key)
    print(obj.size / 1024 / 1024) 

def get_all_s3_objects(s3, **base_kwargs):
    continuation_token = None
    while True:
        list_kwargs = dict(MaxKeys=1000, **base_kwargs)
        if continuation_token:
            list_kwargs['ContinuationToken'] = continuation_token
        response = s3.list_objects_v2(**list_kwargs)
        yield from response.get('Contents', [])
        if not response.get('IsTruncated'):
            break
        continuation_token = response.get('NextContinuationToken')

def first_column_count(filepath):
    with open(filepath, 'r+') as fs:
        for line in fs:
            col = line.split('/')[0]
            tmpdict[col] += float(line.split()[1]) / 1024 / 1024 /1024
    return tmpdict

if __name__ == '__main__':
    filepath = ""
    countfile = ""
    tmpdict = defaultdict(float)
    with open(filepath, 'a+') as fs:
        for n in get_all_s3_objects(boto3.client('s3'), Bucket='elemental', Prefix='kmdm'):
            if not n['Key'].endswith("/") and not n['Key'].endswith("$folder$") and not n['Key'].endswith("_SUCCESS"):
                fs.write(f"{n['Key'].replace('kmdm/', '')} {n['Size']}\n")

    with open(countfile, "a+") as fs:
        for k, v in first_column_count(filepath).items():
            fs.write(f"{k} {v:.3f}G\n")

