#!/usr/bin/env python3.6

import pymysql
import time
from matplotlib import pyplot

class MysqlDb():
    def __init__(self, host, database, password, port=3306, user="root"):
        self.host = host
        self.database = database
        self.password = password
        self.port = port
        self.user = user
    def connect(self):
        try:
            self.dbs = pymysql.connect(host=self.host, db=self.database, port=self.port, user=self.user, password=self.password)
        except pymysql.err.OperationalError:
            print("connect wrong")
        self.cur = self.dbs.cursor()
        return True

    def close(self):
        if self.dbs and self.cur:
            self.dbs.close()
            self.cur.close()
        return True

    def createdb(self):
        pass

    def execute(self, sql):
        self.connect()
        try:
            self.cur.execute(sql)
            self.dbs.commit()
            #return self.cur.fetchall()[0][0]
        except:
            print("sql wrong")
            self.dbs.rollback()
            self.close()

    def get_tables(self):
        sql = "show tables;"
        try:
            self.cur.execute(sql)
            return [table[0] for table in cursor if table]
        except:
            print("something wrong")

    def get_table_column_name(self, table_name):
        sql = "show full columns from %s" % table_name
        return [n[0] for n in self.execute_column(sql) if n]

    def genTimeList(self):
        nd = time.strftime("%Y-%m-%d", time.localtime(int(time.time())))
        time24=[(f'{nd} 00:00:00', f'{nd} 01:00:00'), (f'{nd} 01:00:00', f'{nd} 02:00:00'),
                (f'{nd} 02:00:00', f'{nd} 03:00:00'), (f'{nd} 03:00:00', f'{nd} 04:00:00'),
                (f'{nd} 04:00:00', f'{nd} 05:00:00'), (f'{nd} 05:00:00', f'{nd} 06:00:00'),
                (f'{nd} 06:00:00', f'{nd} 07:00:00'), (f'{nd} 07:00:00', f'{nd} 08:00:00'),
                (f'{nd} 08:00:00', f'{nd} 09:00:00'), (f'{nd} 09:00:00', f'{nd} 10:00:00'),
                (f'{nd} 10:00:00', f'{nd} 11:00:00'), (f'{nd} 11:00:00', f'{nd} 12:00:00'),
                (f'{nd} 12:00:00', f'{nd} 13:00:00'), (f'{nd} 13:00:00', f'{nd} 14:00:00'),
                (f'{nd} 14:00:00', f'{nd} 15:00:00'), (f'{nd} 15:00:00', f'{nd} 16:00:00'),
                (f'{nd} 16:00:00', f'{nd} 17:00:00'), (f'{nd} 17:00:00', f'{nd} 18:00:00'),
                (f'{nd} 18:00:00', f'{nd} 19:00:00'), (f'{nd} 19:00:00', f'{nd} 20:00:00'),
                (f'{nd} 20:00:00', f'{nd} 21:00:00'), (f'{nd} 21:00:00', f'{nd} 22:00:00'),
                (f'{nd} 22:00:00', f'{nd} 23:00:00'), (f'{nd} 23:00:00', f'{nd} 24:00:00')]
        tmplist = []
        for t in time24:
            tmp = self.execute(f'select COUNT(*) from JOBS where END_TIME > "{t[0]}" and END_TIME < "{t[1]}";')
            tmplist.append(tmp) 
        return tmplist
