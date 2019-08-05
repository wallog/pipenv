#!/usr/bin/env python3.6

from influxdb import InfluxDBClient
import time
from progressbar import ProgressBar as PB
from colorama import Fore, Back, Style


class Influxdb():
    def __init__(self, host, port=8086, user="root", password=None, db=None):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.db = db

    def connect(self):
        try:
            self.client = InfluxDBClient(self.host, self.port, self.user, self.password, self.db)
        except:
            print("connect failed!")
        return None

    def getTimeListbak(self, sql, col=None):
        self.result = self.client.query(sql)
        if col == "time":
            return [n["time"] for n in self.result.items()[0][1]]
        elif col == "load":
            return [n["load1"] for n in self.result.items()[0][1]]
        else:
            try:
                raise NameError("third parameter is null,time or load")
            except NameError:
                print("wrong")
    def getTimeList(self, sql):
        self.result = self.client.query(sql)
        tmpdict = {}
        if self.result:
            tmpdict["time"] = [n["time"].split("T")[1].split("+")[0] for n in self.result.items()[0][1]]
            tmpdict["load"] = ['%.2f' % (n["free"] / 1024 / 1024 / 1024) for n in self.result.items()[0][1]] 
            return tmpdict
        else:
            return "bad result"

def changetime(t = 8):
    now = int(time.time() - t * 3600)
    befor = int(now - 3600)
    strtime = time.strftime("%Y-%m-%dT%H:%M:%MZ", time.localtime(befor))
    return strtime


if __name__ == '__main__':
    tt = changetime()
    pbar = PB(maxval=10)
    pbar.start()
    print(Fore.GREEN)
    for i in range(1, 11):
        pbar.update(i)
        time.sleep(0.1)
    pbar.finish() 
    print(Style.RESET_ALL)
