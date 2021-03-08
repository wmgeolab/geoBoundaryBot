import os
import sys
import time
import requests


cQuery = str(sys.argv[2])
level = str(sys.argv[3])
buildType = str(sys.argv[4])
key = str(sys.argv[1])

def run_query(query, key): 
    try:
        headers = {"Authorization": "Bearer %s" % key}
        request = requests.post('https://api.github.com/graphql', json={'query': query}, headers=headers)
        return request.json()
    except Exception as e:
        return(e)

sourceQuery = """
            {
                repository(owner: \"wmgeolab\", name: \"gbRelease\") {
                object(expression: \"master\") {
                    ... on Commit {
                    blame(path: \"sourceData/"""+buildType+"""/"""+cQuery+"""_"""+level+""".zip\") {
                        ranges {
                        commit {
                            committedDate
                        }
                        }
                    }
                    }
                }
                }
            }
            """
result = run_query(sourceQuery, key)

buildQuery = """
            {
                repository(owner: \"wmgeolab\", name: \"gbRelease\") {
                object(expression: \"master\") {
                    ... on Commit {
                    blame(path: \"releaseData/"""+buildType+"""/"""+cQuery+"""/"""+level+"""/geoBoundaries-"""+cQuery+"""-"""+level+""".geojson\") {
                        ranges {
                        commit {
                            committedDate
                        }
                        }
                    }
                    }
                }
                }
            }
            """
sysExit = 0
try:
    commitDate = result["data"]["repository"]["object"]["blame"]["ranges"][0]["commit"]["committedDate"]
    print("Most recent source file is from " + commitDate + ".  Contrasting to build.")
    try:
        buildResult = run_query(buildQuery, key)
        buildDate = buildResult["data"]["repository"]["object"]["blame"]["ranges"][0]["commit"]["committedDate"]
        print("Most recent build is from " + buildDate +".")
        if(buildDate > commitDate):
            print("Build is already up-to-date.  No action needed.")
            sysExit = 1
        else:
            print("Source is newer than build data.  Commencing new build.")
    except:
        print("No build file for this layer currently exists in the repository.  Commencing new build.")

except:
    print("No source file for this layer currently exists in the repository. Skipping any further action.")
    sys.exit("1")

if(sysExit == 1):
    sys.exit("1")

