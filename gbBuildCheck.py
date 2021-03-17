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

def findDate(queryResponse):
    for i in range(0, len(queryResponse["data"]["repository"]["object"]["blame"]["ranges"])):
        curDate = queryResponse["data"]["repository"]["object"]["blame"]["ranges"][i]["commit"]["committedDate"]
        if(i == 0):
            recentDate = curDate
        else:
            if(recentDate < curDate):
                recentDate = curDate
    
    return(recentDate)

sourceQuery = """
            {
                repository(owner: \"wmgeolab\", name: \"geoBoundaries\") {
                object(expression: \"main\") {
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
                repository(owner: \"wmgeolab\", name: \"geoBoundaries\") {
                object(expression: \"main\") {
                    ... on Commit {
                    blame(path: \"releaseData/"""+buildType+"""/"""+cQuery+"""/"""+level+"""/geoBoundaries-"""+cQuery+"""-"""+level+"""-all.zip\") {
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

codeQuery = """
            {
                repository(owner: \"wmgeolab\", name: \"geoBoundaryBot\") {
                object(expression: \"main\") {
                    ... on Commit {
                    blame(path: \"gbBuild.py\") {
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
print(result)
sysExit = 0
try:
    commitDate = findDate(result)
    print("Most recent source file is from " + commitDate + ".  Contrasting to build.")
    try:
        buildResult = run_query(buildQuery, key)
        buildDate = findDate(buildResult)
        print("Most recent build is from " + buildDate +".")
        if(buildDate > commitDate):
            print("Build is already up-to-date.  Confirming build script has not updated.")
            codeResult = run_query(codeQuery, key)
            codeDate = findDate(codeResult)
            print("Most recent code is from " + codeDate)
            if(buildDate > codeDate):
                print("Build is up-to-date with most recent build script.  No further actions necessary.")
                sysExit = 1
            else:
                print("Build script has been updated.  Re-running build.")
        else:
            print("Source is newer than build data.  Commencing new build.")
    except:
        print("No build file for this layer currently exists in the repository.  Commencing new build.")

except:
    print("No source file for this layer currently exists in the repository. Skipping any further action.")
    sys.exit("No source file in the repository.")

if(sysExit == 1):
    sys.exit("Build is already up to date.")

