#!/usr/bin/env python3.6

import requests
import json

private_url = f"{url}/v2/_catalog"
header = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}

def getImageTag(projectname):
    result = requests.get(f"{url}/v2/{projectname}/tags/list")
    result = json.loads(result.text)
    return result["tags"]
def getSha(pn, tag):
    header = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
    result = requests.get(f"{url}/v2/{pn}/manifests/{tag}", headers=header)
    r = result.headers
    return r["Etag"]
def deleteSha(pn, tag):
    header = {"Accept": "application/vnd.docker.distribution.manifest.v2+json"}
    sha = getSha(pn, tag).strip("\"")
    try:
        requests.request("delete", f"{url}/v2/{pn}/manifests/{sha}", headers=header)
    except:
        print("somthing wrong!")
    else:
        print("delete sueccessful")

if __name__ == '__main__':
    deleteSha(f"{repo}", f"{tag}")
