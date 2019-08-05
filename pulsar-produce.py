#!/usr/bin/env python3

import pulsar

client = pulsar.Client("")
try:
    producer = client.create_producer('wl-test')
    for n in range(10):
        producer.send("WangLong Test!".encode('utf-8'))
except:
    print("somthing wrong")
finally:
    client.close()

