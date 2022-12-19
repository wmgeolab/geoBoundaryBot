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
COMMIT = True
CSV_CONSTRUCT = True
#===

GA_PATH = GB_DIR + "/.gitattributes"

with open(LOG_DIR + "status", 'w') as f:
    f.write("GITATTRIBUTE BUILD HAS COMMENCED.")

#Clear old attributes file
try:
    os.remove(GA_PATH)
except:
    pass

for (path, dirname, filenames) in os.walk(GB_DIR):
    if(GB_DIR + "/.git/" not in path):
        for f in filenames:
            fPath = path + "/" + f
            if(os.path.getsize(fPath) > 100000000):
                print("File greater than 100MB Detected: " + str(fPath))
                relPath = "/".join(fPath.strip("/").split('/')[5:])
                print(relPath)
                with open(GA_PATH, "a") as gaP:
                    gaP.write(relPath + " filter=lfs diff=lfs merge=lfs -text" + "\n")


if(COMMIT == True):
    #Commit the build with a stamp and description
    with open(LOG_DIR + "status", 'w') as f:
        f.write("COMMIT AND CALCULATE HASH START.")
    cMes = "Automated Build " + time.ctime()
    commit = "cd " + GB_DIR + "; git add -A .; git commit -m '" + str (cMes) + "'"
    gitResponse = subprocess.Popen(commit, shell=True).wait()
    print(gitResponse)

    gitIDCall = "cd " + GB_DIR + "; git rev-parse HEAD"
    commitIDB = subprocess.check_output(gitIDCall, shell=True)
    gitHash = str(commitIDB.decode('UTF-8'))
else:
    gitHash = "TESTHASH"

if(len(gitHash) < 5):
    with open(LOG_DIR + "status", 'w') as f:
        f.write("ERROR: gitHash invalid.")
    print("ERROR: gitHash invalid.")
    sys.exit()

if(CSV_CONSTRUCT == True):
    with open(LOG_DIR + "status", 'w') as f:
        f.write("HASH CALCULATED: " + str(gitHash) + " COMMENCING CSV CONSTRUCTION.")

    TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilder/"
    GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"

    #Load in the master ISO lookup table
    isoDetails = pd.read_csv("../dta/iso_3166_1_alpha_3.csv", encoding='utf-8')

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
        f.write("boundaryID,boundaryName,boundaryISO,boundaryYearRepresented,boundaryType,boundaryCanonical,boundarySource,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,apiURL,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,staticDownloadLink\n")

    with open(gbOpenCSV,'w+') as f:
        headerWriter(f)

    with open(gbHumCSV,'w+') as f:
        headerWriter(f)

    with open(gbAuthCSV,'w+') as f:
        headerWriter(f)

    for (path, dirname, filenames) in os.walk(GB_DIR + "releaseData/"):
        with open(LOG_DIR + "status", 'w') as f:
            f.write(path)
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
        print(metaSearch)
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

            metaLine = metaLine + "https://www.geoboundaries.org/api/gbID/" + meta['boundaryID'] + '","'

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
            print("Error - multiple returns from metaSearch!")