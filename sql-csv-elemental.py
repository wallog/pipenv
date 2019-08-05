#!/usr/bin/env python3

import pymysql
import os
import codecs
import csv
import requests
import boto3
import time
from multiprocessing import Pool, Manager
from progressbar import ProgressBar as PB
from colorama import Fore, Back, Style

def probar(fun):
    def wrapper(*args, **kwargs):
            pbar = PB(maxval=10)
            pbar.start()
            print(Fore.GREEN)
            start_time = int(time.time())
            fun(*args, **kwargs)
            end_time = int(time.time())
            for i in range(1, end_time-start_time):
                pbar.update(i)
                time.sleep(0.1)
            pbar.finish()
            print(Style.RESET_ALL)
    return wrapper

class Mysqlinit():
    def __init__(self, host, db, password, port=3306, user="root"):
        self.db = db
        try:
            self.connect = pymysql.connect(host=host, db=db, port=port, user=user, password=password, cursorclass= pymysql.cursors.SSCursor)
        except Exception as e:
            print(f"[error]: {e}")

    def createdb(self):
        pass
   
    def execute_column(self, sql):
        cursor = self.connect.cursor()
        cursor.execute(sql)
        return cursor
    def get_tables(self):
        sql = "show tables;"
        try:
            cursor = self.connect.cursor()
            cursor.execute(sql)
        except Exception as e:
            print(e)
        else:
            return [table[0] for table in cursor if table]
  
    def get_table_column_name(self, table_name):
        sql = "show full columns from %s" % table_name
        return [n[0] for n in self.execute_column(sql) if n]

@probar
def import_csv(db, columnhead, sql, filename):
    with codecs.open(filename=filename, mode="a", encoding="utf-8") as f:
        write = csv.writer(f, dialect="excel")
        write.writerow(columnhead)
        for col in db.execute_column(sql):
            write.writerow(col)

def mktmpdir(csvdir):
    try:
        os.mkdir(csvdir)
    except FileExistsError:
        print("file exist!")
    return csvdir

def getfilesize(path,filename):
    return os.path.getsize(path+filename)

def postfile(projectId, filename, filesize):
    baseurl = "%s" % projectId
    data = {"name": filename, "path": "/wltest/%s" % filename, "fileSize": filesize}
    r = requests.get(baseurl, params = data)
    return r.url
    
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
