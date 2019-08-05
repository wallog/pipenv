#!/usr/bin/env python3.6

import kubernetes
from kubernetes import client, config
from collections import defaultdict

def main():
    body = {
        "metadata": {
            "labels": {
                "spark-role": "driver"}
        }
    }

    return [ n.metadata.name for n in api_response.items]
def logname(func):
    def logn(*args, **kwargs):
        na = func.__name__
        print("Call fun name: %s" % na)
        print("Refresh context")
        return func(*args, **kwargs)
    return logn

def loggin(level):
    def second(func):
        def third(*args, **kwargs):
            fn = func.__name__
            print("[%s]: Call func name is: %s " % (level, fn))
            return func(*args, **kwargs)
        return third
    return second

def numContext(label= "spark-role=executor"):
    contextList= []
    api_response = api_instance.list_pod_for_all_namespaces(label_selector=label)
    for i in api_response.items:
        contextList.append(i.metadata.name)
    return len(contextList)

def getPodinfo(namespace="default"):
    api_response = api_instance.list_namespaced_pod(namespace)
    for i in api_response.items:
        if i.status.container_statuses[0].state.running:
            print(i.status.pod_ip, i.metadata.name, i.status.container_statuses[0].image, "running")
        else:
            print(i.status.pod_ip, i.metadata.name, i.status.container_statuses[0].image, "stopped")

def getPodname(name, namespace="default"):
    api_response = api_instance.list_namespaced_pod(namespace)
    for i in api_response.items:
        if name in i.metadata.name:
            return i.metadata.name

def getServiceIP(names="spark-jobserver"):
    api_response = api_instance.list_service_for_all_namespaces()
    for i in api_response.items:
        if i.metadata.name == names:
           servicedict[names] = i.spec.cluster_ip
    return servicedict[names]

def patchpod(name, tag, namespace="default"):
    body = [{"op": "replace", "path": "/spec/containers/0/image", "value": tag}]
    podname = getPodname(name)
    api_response = api_instance.patch_namespaced_pod(podname, namespace, body)
    print(api_response)

#@logname
@loggin(level="INFO")
def refreshScheduler(name= "aco-sheduler",method= "POST", resource= "refresh"):
    c= pycurl.Curl()
    c.setopt(c.URL, getServiceIP(names= name)+ ":9091/"+ resource)
    c.setopt(pycurl.CUSTOMREQUEST, method)
    c.perform()
    c.close()
    return None
def deleteContext(name, namespace= "default", body = kubernetes.client.V1DeleteOptions()):
    api_instance.delete_namespaced_pod(name, namespace, body) 
    return None
if __name__ == '__main__':
    servicedict = {}
    config.load_kube_config()
    api_instance = client.CoreV1Api()
    init_context = 5
    getPodinfo()
# judge contexts,if less 5;then POST "refresh" to scheduler
    patchpod("", body)
    try:
        if numContext() != init_context:
            refreshScheduler()
    except NameError as e:
        print("Module: pycurl not found!", e)
    deleteContext("standard-1")
