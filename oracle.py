#!/usr/bin/env python3.6
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import cx_Oracle
import time
import csv
from retrys import retry

class OracleOP:
    """
       oracle test object
    """

    __slot__ = "user", "password", "host", "instance"

    def __init__(self,user, password, host, instance):
        self.user = user
        self.password = password
        self.host = host
        self.instance = instance
    def __repr__(self):
        return "class : Oracle, property: user, password, host, instance"

    def __getattr__(self, key):
        print(f"{key}---wrong")
    
    def conn(self):
        try:
            self.con = cx_Oracle.connect(self.user+'/'+self.password+'@'+self.host+'/'+self.instance)
        except:
            print("connect somthing wrong!")
            self.con.close()
        else:
            return self.con

    def close(self):
        self.con.close() 
 
    def select(self, sql, num=None):
        cur = self.con.cursor()
        cur.arraysize = 2000
        try:
            cur.execute(sql)
        except:
            print("sql bad!")
        else:
            if not num:
                res = cur.fetchall()
                return res
            else:
                try:
                    res = cur.fetchmany(numRows=num)
                    res1 = cur.fetchmany(numRows=num)
                except:
                    print("num may be wrong")
                else:
                    return res, res1
        finally:
            cur.close()

    def desc(self, tablename):
        tablename = tablename.upper()
        cur = self.con.cursor()
        try:
            cur.execute(f"select COLUMN_NAME from USER_COL_COMMENTS where TABLE_NAME = '{tablename}'")
        except cx_Oracle.DatabaseError as e:
            print(e)
        except:
            print(f"{tablename} not exist")
        else:
            res = cur.fetchall()
            col = list(map(lambda x:x[0], res))
            num = len(col)
            return col
        finally:
            cur.close()

def citime(func):
    def wrapper(*args, **kwargs):
        starttime = int(time.time())
        func(*args, **kwargs)
        endtime = int(time.time())
        print(f"genarate parquet file pay {endtime-starttime}s")
    return wrapper

@citime
@retry
def gen_parquet(conn, tablename, parname):
    try:
        df = pd.read_sql(f"select * from {tablename}", con=conn)
    except NameError:
        print("sql wrong")
    else:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parname)
        return df.count()
@citime
def gen_csv(conn, tablename, header):
    cur = conn.cursor()
    cur.arraysize = 2000
    try:
        cur.execute(f"select * from {tablename}")
    except Exception as e:
        print(f"[error]: {e}")
    else:
        try:
            res = cur.fetchall()
        except Exception as e:
            print(f"[error]: {e}")
        else:
            with open(f"{tablename}.csv", "a+") as tc:
                mycsv = csv.writer(tc)
                mycsv.writerow(header)
                mycsv.writerows(res)

def connection_test(conn):
    if conn.version:
        print("connect successfule!")
    else:
        print("somthing wrong")
