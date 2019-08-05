#!/usr/bin/env python3.6

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from oracle import OracleOP as op

d = defaultdict(list)
o10 = op()
df = pd.read_sql("select * from mrtab", con=o10.conn())
o10.close()
table = pa.Table.from_pandas(df)
pq.write_table(table, '')

def gen_parquet(conn, tablename, parname):
    try:
        df = pd.read_sql(f"select * from {tablename}", con=conn)
    except:
        print("sql wrong")
    else:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, parname)
    finally:
        con.close()
        

def gen_csv(conn, tablename, csvname):
    try:
        df = pd.read_sql(f"select * from {tablename}", con=conn)
    except Exception as e:
        print(f"[error]: {e}")
    else:
        table = pa.Table.from_pandas(df)
        pq.write_table(table, csvname)
    finally:
        con.close()
