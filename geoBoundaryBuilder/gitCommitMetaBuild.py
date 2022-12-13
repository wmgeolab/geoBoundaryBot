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

with open(LOG_DIR + "status", 'w') as f:
    f.write("HASH CALCULATED: " + str(gitHash) + " COMMENCING CSV CONSTRUCTION.")

TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilder/"
GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"

#Load in the master ISO lookup table
isoDetails = pd.read_csv("../dta/iso_3166_1_alpha_3.csv", encoding='utf-8')

#Define Paths
gbOpenCSV = GB_DIR + "releaseData/geoBoundariesOpen-meta.csv"
gbHumCSV = GB_DIR + "releaseData/UNOCHA-meta.csv"
gbAuthCSV = GB_DIR + "releaseData/UNSALB-meta.csv"

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
        releaseType = "UNOCHA"
    elif("gbOpen" in path):
        csvPath = gbOpenCSV
        releaseType = "gbOpen"
    elif("gbAuthoritative" in path):
        csvPath = gbAuthCSV
        releaseType = "UNSALB"
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

        print("Preparing to calculate geometry statistics.")
        #Calculate geometry statistics
        #We'll use the geoJSON here, as the statistics (i.e., vertices) will be most comparable
        #to other cases.
        #Build geoJSON link
        gJLink = GB_DIR + "releaseData/" + releaseType + "/" + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + ".geojson"
       
        geom = geopandas.read_file(gJLink)

        admCount = len(geom)
        
        vertices=[]
        
        for i, row in geom.iterrows():
            n = 0
            if(row.geometry.type.startswith("Multi")):
                for seg in row.geometry:
                    n += len(seg.exterior.coords)
            else:
                n = len(row.geometry.exterior.coords)
            
            vertices.append(n) ###

metaLine = metaLine + str(admCount) + '","' + str(round(sum(vertices)/len(vertices),0)) + '","' + str(min(vertices)) + '","' + str(max(vertices)) + '","'
        
        # DEBUG
        if len(vertices) == 0:
            print('Error: Empty file?', geom, len(geom), vertices, len(list(geom.iterrows())) )
            print("Falling back to JSON.")
            try:
                geom = requests.get(gJLink).json()["features"]
                vertices = 0
                for i in range(0,len(geom[0]["geometry"]["coordinates"])):
                    vertices += len(geom[0]["geometry"]["coordinates"][i][0])
            except:
                print("JSON fallback failed.")
            
            metaLine = metaLine + str(admCount) + '","' + str(round(vertices,0)) + '","' + str(vertices) + '","' + str(vertices) + '","'
        else:
            metaLine = metaLine + str(admCount) + '","' + str(round(sum(vertices)/len(vertices),0)) + '","' + str(min(vertices)) + '","' + str(max(vertices)) + '","'
            
        


        #Perimeter Using WGS 84 / World Equidistant Cylindrical (EPSG 4087)
        try:
            lengthGeom = geom.copy()
            lengthGeom = lengthGeom.to_crs(epsg=4087)
            lengthGeom["length"] = lengthGeom["geometry"].length / 1000 #km
            metaLine = metaLine + str(lengthGeom["length"].mean()) + '","' + str(lengthGeom["length"].min()) + '","' + str(lengthGeom["length"].max()) + '","'
        except:
            metaLine = metaLine + "" + '","' + "" + '","' + "" + '","'

        #Area #mean min max Using WGS 84 / EASE-GRID 2 (EPSG 6933)
        try:
            areaGeom = geom.copy()
            areaGeom = areaGeom.to_crs(epsg=6933)
            areaGeom["area"] = areaGeom['geometry'].area / 10**6 #sqkm
            
            metaLine = metaLine + str(areaGeom['area'].mean()) + '","' + str(areaGeom['area'].min()) + '","' + str(areaGeom['area'].max()) + '","'
        except:
            metaLine = metaLine + "" + '","' + "" + '","' + "" + '","'
            
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
        
        #Cleanup for memory
        del metaLine
        del geom

        try:
            del lengthGeom
            del areaGeom
        except:
            pass



    else:
        print("Error - multiple returns from metaSearch!")