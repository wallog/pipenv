#!/usr/bin/env python3

import pulsar

client = pulsar.Client('')
consumer = client.subscribe('', '')
msg = consumer.receive()
msg_id = msg.message_id()


while True:
    #msg = reader.receive()
    msg = consumer.receive()
    print("*"*30)
    print("Received message %s, id = %s" % (msg.data(), msg.message_id()))
    print("*"*30)
    consumer.acknowledge(msg)

client.close()
