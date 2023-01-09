import os
import sys 
import time
import subprocess
import pandas as pd
import re
import json
import geopandas

#This script prepares the *.gitattributes file for git lfs.
#We check if any files are >100MB, and if so tag them for LFS inclusion.

#source "/usr/local/anaconda3-2021.05/etc/profile.d/conda.csh"
#module load anaconda3/2021.05
#module load git-lfs/3.2.0
#unsetenv PYTHONPATH
#conda activate geoBoundariesBuild

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries"
LOG_DIR = "/sciclone/geograd/geoBoundaries/logs/gbBuilderCommitCSV/"
STAT_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderWatch/"
STAGE_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderStage/"
TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilder/"
BOT_DTA = "/sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/dta/"
COMMIT = False
CSV_CONSTRUCT = True
#===

def statusUpdate(ISO, ADM, product, code):
    with open(STAT_DIR + "_" + ISO + "_" + ADM + "_" + product, 'w') as f:
        f.write(code)

with open(STAGE_DIR + "buildStatus", 'w') as f:
    f.write("STARTING COMMITS.")

if(COMMIT == True):
    #Commit the build with a stamp and description
    #Note we'll be doing this folder-by-folder,
    #as we need to commit each layer individually to keep 
    #the git pushes small. 
    #We then save the hash of the final commit, as that will have 
    #all relevant new files contained in it (for linking in the metadata and API).
    with open(LOG_DIR + "status", 'a') as f:
        f.write("COMMIT AND CALCULATE HASH START.\n")

    buildTypes = ["gbOpen", "gbAuthoritative", "gbHumanitarian"]
    totalISO = len(os.listdir(GB_DIR + "/releaseData/gbOpen/")) + len(os.listdir(GB_DIR + "/releaseData/gbAuthoritative/")) + len(os.listdir(GB_DIR + "/releaseData/gbHumanitarian/"))
    committedISO = 0
    for buildType in buildTypes:
        for ISO in os.listdir(GB_DIR + "/releaseData/" + buildType + "/"):
            with open(STAGE_DIR + "buildStatus", 'w') as f:
                f.write("Committing Files: " + str(committedISO) + " of " + str(totalISO) + " (" + ISO + ")")
            with open(LOG_DIR + "status", 'a') as f:
                f.write("Committing Files: " + str(committedISO) + " of " + str(totalISO) + " (" + ISO + ")" + "\n")
            
            for ADM in os.listdir(GB_DIR + "/releaseData/" + buildType + "/" + ISO + "/"):
                with open(LOG_DIR + "status", 'a') as f:
                    f.write("============="+str(ISO) + " " + str(ADM)+"\n")
                
                statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="+P")
                newDataCheck = subprocess.check_output(["cd /sciclone/geograd/geoBoundaries/database/geoBoundaries/releaseData/"+buildType+"/"+ISO+"/"+ADM+"; git status ."], shell=True).decode()
                with open(LOG_DIR + "status", 'a') as f:
                    f.write(str(newDataCheck) + "\n")

                if("nothing to commit" not in newDataCheck):
                    cMes = "Automated Build Commit for " + ISO + " | " + ADM + " | " + buildType + " | Committed on: " + time.ctime()
                    statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="+A")
                    addData = subprocess.check_output(["cd /sciclone/geograd/geoBoundaries/database/geoBoundaries/releaseData/"+buildType+"/"+ISO+"/"+ADM+"; git add -A ."], shell=True).decode()
                    with open(LOG_DIR + "status", 'a') as f:
                            f.write(str(addData) + "\n")
                    if(("error" not in addData) and ("fatal" not in addData)):
                        statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="+C")
                        commitData = subprocess.check_output(["cd /sciclone/geograd/geoBoundaries/database/geoBoundaries/releaseData/"+buildType+"/"+ISO+"/"+ADM+"; git commit -m '"+str(cMes)+"'"], shell=True).decode()
                        with open(LOG_DIR + "status", 'a') as f:
                            f.write(str(commitData) + "\n")
                        if(("error" not in commitData) and ("fatal" not in commitData)):
                            statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="+D")
                            with open(LOG_DIR + "status", 'a') as f:
                                f.write("Commit successful.\n")
                        else:
                            statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="-E")
                            with open(LOG_DIR + "status", 'a') as f:
                                f.write("Commit failed.\n")
                    else:
                        statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="-E")
                else:
                    statusUpdate(ISO=ISO, ADM=ADM, product=buildType, code="+S")
                    with open(LOG_DIR + "status", 'a') as f:
                        f.write("No changes found, skipping commit.\n")
                
            committedISO += 1
    with open(STAGE_DIR + "buildStatus", 'w') as f:
        f.write("ALL COMMITS DONE, FINDING FINAL HASH.")


    gitIDCall = "cd " + GB_DIR + "/releaseData/" + "; git log -n 1 --pretty=format:'%h' -p -- " + ":./geoBoundariesOpen-meta.csv"
    commitIDB = subprocess.check_output(gitIDCall, shell=True)
    gitHash = str(commitIDB.decode('UTF-8').split("\n")[0])

else:
    gitIDCall = "cd " + GB_DIR + "/releaseData/" + "; git log -n 1 --pretty=format:'%h' -p -- " + ":./geoBoundariesOpen-meta.csv"
    commitIDB = subprocess.check_output(gitIDCall, shell=True)
    gitHash = str(commitIDB.decode('UTF-8').split("\n")[0])

if(len(gitHash) < 5):
    with open(LOG_DIR + "status", 'a') as f:
        f.write("ERROR: gitHash invalid." + str(gitHash))
    with open(STAGE_DIR + "buildStatus", 'w') as f:
        f.write("ERROR IN RETRIEVING HASH, INVALID.")
    sys.exit()
else:
    with open(STAGE_DIR + "buildStatus", 'w') as f:
        f.write("HASH CALCULATED: " + str(gitHash) + " COMMENCING CSV CONSTRUCTION.")

if(CSV_CONSTRUCT == True):
    with open(LOG_DIR + "status", 'a') as f:
        f.write("==============COMMENCING CSV CONSTRUCTION==============\n")

    #Adding trailing slash - git required we skip it in the original definition.
    GB_DIR = GB_DIR + "/"

    #Load in the master ISO lookup table
    isoDetails = pd.read_csv(BOT_DTA + "iso_3166_1_alpha_3.csv", encoding='utf-8')

    #Define Paths
    gbOpenCSV = GB_DIR + "releaseData/geoBoundariesOpen-meta.csv"
    gbHumCSV = GB_DIR + "releaseData/geoBoundariesHumanitarian-meta.csv"
    gbAuthCSV = GB_DIR + "releaseData/geoBoundariesAuthoritative-meta.csv"

    #Remove any old versions of files
    try:
        os.remove(gbOpenCSV)
    except:
        pass

    try:
        os.remove(gbHumCSV)
    except:
        pass

    try:
        os.remove(gbAuthCSV)
    except:
        pass

    #Create headers for each CSV
    def headerWriter(f):
        f.write("boundaryID,boundaryName,boundaryISO,boundaryYearRepresented,boundaryType,boundaryCanonical,boundarySource,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,staticDownloadLink\n")

    with open(gbOpenCSV,'w+') as f:
        headerWriter(f)

    with open(gbHumCSV,'w+') as f:
        headerWriter(f)

    with open(gbAuthCSV,'w+') as f:
        headerWriter(f)

    for (path, dirname, filenames) in os.walk(GB_DIR + "releaseData/"):
        with open(LOG_DIR + "status", 'a') as f:
            f.write(path + "\n")
        if("gbHumanitarian" in path):
            csvPath = gbHumCSV
            releaseType = "gbHumanitarian"
        elif("gbOpen" in path):
            csvPath = gbOpenCSV
            releaseType = "gbOpen"
        elif("gbAuthoritative" in path):
            csvPath = gbAuthCSV
            releaseType = "gbAuthoritative"
        else:
            continue
            
        metaSearch = [x for x in filenames if re.search('metaData.json', x)]
        if(len(metaSearch)==1):
            print("Loading JSON: " + path + "/" + metaSearch[0])
            with open(path + "/" + metaSearch[0], encoding='utf-8', mode="r") as j:
                meta = json.load(j)
            print("JSON loaded")
            
            isoMeta = isoDetails[isoDetails["Alpha-3code"] == meta['boundaryISO']]
            #Build the metadata
            metaLine = '"' + meta['boundaryID'] + '","' + isoMeta["Name"].values[0] + '","' + meta['boundaryISO'] + '","' + meta['boundaryYear'] + '","' + meta["boundaryType"] + '","'

            if("boundaryCanonical" in meta):
                if(len(meta["boundaryCanonical"])>0):
                    metaLine = metaLine + meta["boundaryCanonical"] + '","'
                else:
                    metaLine = metaLine + 'Unknown","'
            else:
                metaLine = metaLine + 'Unknown","'

            #Cleanup free-form text fields
            meta['licenseDetail'] = meta["licenseDetail"].replace(',','')
            meta['licenseDetail'] = meta["licenseDetail"].replace('\\','')
            meta['licenseDetail'] = meta["licenseDetail"].replace('"','')

            metaLine = metaLine + meta['boundarySource'] + '","' + meta['boundaryLicense'] + '","' + meta['licenseDetail'].replace("https//","").replace("https://","").replace("http//","").replace("http://","") + '","' + meta['licenseSource'].replace("https//","").replace("https://","").replace("http//","").replace("http://","")  + '","'
            metaLine = metaLine + meta['boundarySourceURL'].replace("https//","https://").replace("https://","").replace("http//","").replace("http://","")  + '","' + meta['sourceDataUpdateDate'] + '","' + meta["buildDate"] + '","'
            
            
            metaLine = metaLine + isoMeta["Continent"].values[0] + '","' + isoMeta["UNSDG-region"].values[0] + '","'
            metaLine = metaLine + isoMeta["UNSDG-subregion"].values[0] + '","' 
            metaLine = metaLine + isoMeta["worldBankIncomeGroup"].values[0] + '","'

            #Append geometry stats
            metaLine = metaLine + str(meta["admUnitCount"]) + '","' + str(meta["meanVertices"]) + '","' + str(meta["minVertices"]) + '","' + str(meta["maxVertices"]) + '","'

            metaLine = metaLine + str(meta["meanPerimeterLengthKM"]) + '","' + str(meta["minPerimeterLengthKM"]) + '","' + str(meta["maxPerimeterLengthKM"]) + '","'
                
            metaLine = metaLine + str(meta["meanAreaSqKM"]) + '","' + str(meta["minAreaSqKM"]) + '","' + str(meta["maxAreaSqKM"]) + '","'
                
            #Cleanup
            metaLine = metaLine.replace("nan","")

            #Add static link
            metaLine = metaLine + "https://github.com/wmgeolab/geoBoundaries/raw/" + gitHash + "/releaseData/" + releaseType + "/" + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + "-all.zip" + '","'

            #Strip final entry 
            metaLine = metaLine[:-3]

            #Newline
            metaLine = metaLine + '"\n'

            with open(csvPath, mode='a', encoding='utf-8') as f:
                f.write(metaLine)

        else:
            with open(LOG_DIR + "status", 'a') as f:
                f.write("Multiple metasearch returns:" + str(metaSearch) + "\n")
    
    with open(LOG_DIR + "status", 'a') as f:
        f.write("==============CSV CONSTRUCTION COMPLETE==============\n")