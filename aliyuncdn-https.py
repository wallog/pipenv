#!/usr/bin/env python3

from aliyunsdkcore import client
from aliyunsdkcdn.request.v20180510 import RefreshObjectCachesRequest
import sys

Client = client.AcsClient(ID,PD)
url = desurl + sys.argv[1]
def refresh(url=desurl, types="Directory"):
    request = RefreshObjectCachesRequest.RefreshObjectCachesRequest()
    request.set_ObjectPath(url)
    request.set_ObjectType(types)
    x = Client.do_action(request)
    print(x)
    return x
if len(sys.argv) == 2:
    refresh(url=url)
elif len(sys.argv) == 3:
    types = sys.argv[2]
    refresh(url=url, types=types)
else:
    print("involid argv")
