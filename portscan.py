#!/usr/bin/env python3

import socket
import threading
import time
from multiprocessing import Pool
import sys
import pysnooper

@pysnooper.snoop("/root/portscan.log")
def getport(ip, port):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.connect((ip, port))
        print("%s port %s is open" % (ip, port))
    except:
        pass
    finally:
        server.close()

def main(ip):
    print("%s  start........" % time.ctime())
    t1 = int(time.time())
    for port in range(70, 75535):
        getport(ip, port)
    print("%s  end........" % time.ctime())
    t2 = int(time.time())
    print("total time : %d" % (t2 - t1))

def main1(ip):
    threads = []
    print("%s  start........" % time.ctime())
    t1 = int(time.time())
    for port in range(70, 65535):
        t = threading.Thread(target=getport, args=(host, port))
        threads.append(t)
        t.start()
    for t in threads:
        t.join()
    t2 = int(time.time())
    print("%s  end........" % time.ctime())
    print("total time : %d" % (t2 - t1))

def main2(ip):
    p = Pool(10)
    print("%s  start........" % time.ctime())
    t1 = int(time.time())
    for port in range(70, 65535):
        p.apply_async(getport, args = (ip, port))
    p.close()
    p.join()
    t2 = int(time.time())
    print("%s  end........" % time.ctime())
    print("total time : %d" % (t2 - t1))
    

if __name__ == '__main__':
    host = sys.argv[1]
    main2(host)
