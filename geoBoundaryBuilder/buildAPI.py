import pandas as pd
import os
import sys
import json
import numpy as np
import copy
import time
import subprocess

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"
API_DIR = "/sciclone/geograd/geoBoundaries/scripts/gbWeb/api/"

openDta = pd.read_csv(GB_DIR + "releaseData/geoBoundariesOpen-meta.csv", encoding='utf8').astype(str)
humDta = pd.read_csv(GB_DIR + "releaseData/geoBoundariesHumanitarian-meta.csv", encoding='utf8').astype(str)
authDta = pd.read_csv(GB_DIR + "releaseData/geoBoundariesAuthoritative-meta.csv", encoding='utf8').astype(str)

allADM = {}
allADM["gbOpen"] = {}
allADM["gbHumanitarian"] = {}
allADM["gbAuthoritative"] = {}

#These dicts all hold all boundaries for a given level.
#This is to support user queries of ISO=ALL for each level.
allADM["gbOpen"]["ADM0"] = []
allADM["gbOpen"]["ADM1"] = []
allADM["gbOpen"]["ADM2"] = []
allADM["gbOpen"]["ADM3"] = []
allADM["gbOpen"]["ADM4"] = []
allADM["gbOpen"]["ADM5"] = []

allADM["gbHumanitarian"]["ADM0"] = []
allADM["gbHumanitarian"]["ADM1"] = []
allADM["gbHumanitarian"]["ADM2"] = []
allADM["gbHumanitarian"]["ADM3"] = []
allADM["gbHumanitarian"]["ADM4"] = []
allADM["gbHumanitarian"]["ADM5"] = []

allADM["gbAuthoritative"]["ADM0"] = []
allADM["gbAuthoritative"]["ADM1"] = []
allADM["gbAuthoritative"]["ADM2"] = []
allADM["gbAuthoritative"]["ADM3"] = []
allADM["gbAuthoritative"]["ADM4"] = []
allADM["gbAuthoritative"]["ADM5"] = []

#Additionally, for each country we'll add another dictionary.
#This is to support user queries of ADM=ALL for any ISO.
allISO = {}
allISO["gbOpen"] = {}
allISO["gbHumanitarian"] = {}
allISO["gbAuthoritative"] = {}

#Finally, we'll just copy everything into all:
all = {}
all["gbOpen"] = []
all["gbHumanitarian"] = []
all["gbAuthoritative"] = []

def LFSconversion(fPath):
    if(fPath.split("/")[10] in lfsFiles):
        newA = fPath.replace("raw.githubusercontent.com/", "media.githubusercontent.com/media/")
        return(newA)
    else:
        return(fPath)
    

def apiBuilder(GB_DIR, API_DIR,ISO, ADM, PRODUCT, ID, apiDict):
    gbIDPath = API_DIR + "gbID/" + str(ID) + "/"
    currentPath = API_DIR + "current/" + PRODUCT + "/" + str(ISO) + "/" + str(ADM) + "/"
    boundaryPath = GB_DIR + "releaseData/" + PRODUCT + "/" + str(ISO) + "/" + str(ADM) + "/"
    
    os.makedirs(currentPath, exist_ok=True)

    #Retrieve the commit hash for the current version of the boundary
    gitIDCall = "cd " + GB_DIR + "releaseData/" + PRODUCT + "/" + ISO + "/" + ADM + "/; git log -n 1 --pretty=format:'%h' -p -- " + ":./geoBoundaries-" + ISO + "-" + ADM + "-all.zip"
    commitIDB = subprocess.check_output(gitIDCall, shell=True)
    gitHash = str(commitIDB.decode('UTF-8').split("\n")[0])

    apiDict["gjDownloadURL"] = "https://github.com/wmgeolab/geoBoundaries/raw/"+gitHash+"/releaseData/"+PRODUCT+"/"+ISO+"/"+ADM+"/geoBoundaries-"+ISO+"-"+ADM+".geojson"
    apiDict["tjDownloadURL"] = "https://github.com/wmgeolab/geoBoundaries/raw/"+gitHash+"/releaseData/"+PRODUCT+"/"+ISO+"/"+ADM+"/geoBoundaries-"+ISO+"-"+ADM+".topojson"
    apiDict["imagePreview"] = "https://github.com/wmgeolab/geoBoundaries/raw/"+gitHash+"/releaseData/"+PRODUCT+"/"+ISO+"/"+ADM+"/geoBoundaries-"+ISO+"-"+ADM+"-PREVIEW.png"
    apiDict["simplifiedGeometryGeoJSON"] = "https://github.com/wmgeolab/geoBoundaries/raw/"+gitHash+"/releaseData/"+PRODUCT+"/"+ISO+"/"+ADM+"/geoBoundaries-"+ISO+"-"+ADM+"_simplified.geojson"
    apiDict["staticDownloadLink"] = "https://github.com/wmgeolab/geoBoundaries/raw/"+gitHash+"/releaseData/"+PRODUCT+"/"+ISO+"/"+ADM+"/geoBoundaries-"+ISO+"-"+ADM+"-all.zip"
    print(currentPath)
    print(apiDict)

    #Update "Current" API endpoint
    with open(currentPath + "index.json", "w") as f:
        json.dump(apiDict, f)
    
    return(apiDict)

#gbOpen
for i, r in openDta.iterrows():
    apiData = {}
    apiData = r.to_dict()

    ID = r["boundaryID"]
    ISO = r["boundaryISO"]
    ADM = r["boundaryType"]
    PRODUCT = "gbOpen"
    print("Building " + PRODUCT + " " + ISO + " " + ADM + " " + str(ID) + "...")

    builderOutput = apiBuilder(GB_DIR, API_DIR, ISO, ADM, PRODUCT, ID, apiData)

    allADM[PRODUCT][ADM].append(builderOutput)

    if(ISO in allISO[PRODUCT]):
        allISO[PRODUCT][ISO].append(builderOutput)
    else:
        allISO[PRODUCT][ISO] = []
        allISO[PRODUCT][ISO].append(builderOutput)

    all[PRODUCT].append(builderOutput)
    
#gbHumanitarian
for i, r in humDta.iterrows():
    apiData = {}
    apiData = r.to_dict()

    ID = r["boundaryID"]
    ISO = r["boundaryISO"]
    ADM = r["boundaryType"]
    PRODUCT = "gbHumanitarian"

    builderOutput = apiBuilder(GB_DIR, API_DIR, ISO, ADM, PRODUCT, ID, apiData)

    allADM[PRODUCT][ADM].append(builderOutput)

    if(ISO in allISO[PRODUCT]):
        allISO[PRODUCT][ISO].append(builderOutput)
    else:
        allISO[PRODUCT][ISO] = []
        allISO[PRODUCT][ISO].append(builderOutput)

    all[PRODUCT].append(builderOutput)
    
#gbAuthoritative
for i, r in authDta.iterrows():
    apiData = {}
    apiData = r.to_dict()

    ID = r["boundaryID"]
    ISO = r["boundaryISO"]
    ADM = r["boundaryType"]
    PRODUCT = "gbAuthoritative"

    builderOutput = apiBuilder(GB_DIR, API_DIR, ISO, ADM, PRODUCT, ID, apiData)

    allADM[PRODUCT][ADM].append(builderOutput)

    if(ISO in allISO[PRODUCT]):
        allISO[PRODUCT][ISO].append(builderOutput)
    else:
        allISO[PRODUCT][ISO] = []
        allISO[PRODUCT][ISO].append(builderOutput)

    all[PRODUCT].append(builderOutput)
    
          
#Add the "ALL" folders for ADMs and save the relevant jsons
for releaseType in allADM:
    for level in allADM[releaseType]:
        allPath = API_DIR + "current/"+str(releaseType)+"/ALL/" + str(level) + "/"
        os.makedirs(allPath, exist_ok=True)
        outFile = allPath + "index.json"
        with open(outFile, "w") as f:
            json.dump(allADM[releaseType][level], f)  

#Add the "ALL" to each ISO folder, as well as the ALL/ALL
for releaseType in allISO:
    allALLPath = API_DIR + "current/"+str(releaseType)+"/ALL/ALL/"
    os.makedirs(allALLPath, exist_ok=True)
    outALL = allALLPath + "index.json"
    with open(outALL, "w") as f:
        json.dump(all[releaseType], f)

    for iso in allISO[releaseType]:
        allPath = API_DIR + "current/"+str(releaseType)+"/"+str(iso)+"/ALL/" 
        os.makedirs(allPath, exist_ok=True)
        outFile = allPath + "index.json"
        with open(outFile, "w") as f:
            json.dump(allISO[releaseType][iso], f)
