import json
import os
import re
from datetime import datetime

import geopandas
import pandas as pd
import requests

#Initialize workspace
ws = {}
try:
    ws['working'] = os.environ['GITHUB_WORKSPACE']
    ws['logPath'] = os.path.expanduser("~") + "/tmp/log.txt"
except:
    ws['working'] = "/home/dan/git/gbRelease"
    ws['logPath'] = os.path.expanduser("~") + "/tmp/log.txt"

#Load in the ISO lookup table
isoDetails = pd.read_csv(ws['working'] + "/geoBoundaryBot/dta/iso_3166_1_alpha_3.csv",
                        encoding='utf-8')


#Get hash for static links for each boundary
r = requests.get("https://api.github.com/repos/wmgeolab/geoboundaries/commits/main")
gitHash = r.json()["sha"]

#Remove any old CSVs for each case
gbOpenCSV = ws["working"] + "/releaseData/geoBoundariesOpen-meta.csv"
gbHumCSV = ws["working"] + "/releaseData/geoBoundariesHumanitarian-meta.csv"
gbAuthCSV = ws["working"] + "/releaseData/geoBoundariesAuthoritative-meta.csv"

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
    f.write("boundaryID,boundaryName,boundaryISO,boundaryYearRepresented,boundaryType,boundaryCanonical,boundarySource-1,boundarySource-2,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildUpdateDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,apiURL,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,staticDownloadLink\n")

with open(gbOpenCSV,'w+') as f:
    headerWriter(f)

with open(gbHumCSV,'w+') as f:
    headerWriter(f)

with open(gbAuthCSV,'w+') as f:
    headerWriter(f)


for (path, dirname, filenames) in os.walk(ws["working"] + "/releaseData/"):
    print(datetime.now(), path)

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

    print("Meta Search Commencing")
    metaSearch = [x for x in filenames if re.search('metaData.json', x)]
    print(metaSearch)
    if(len(metaSearch)==1):
        print("Loading JSON")
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

        metaLine = metaLine + meta['boundarySource-1'] + '","' + meta['boundarySource-2'] + '","' + meta['boundaryLicense'] + '","' + meta['licenseDetail'].replace("https//","").replace("https://","").replace("http//","").replace("http://","") + '","' + meta['licenseSource'].replace("https//","").replace("https://","").replace("http//","").replace("http://","")  + '","'
        metaLine = metaLine + meta['boundarySourceURL'].replace("https//","https://").replace("https://","").replace("http//","").replace("http://","")  + '","' + meta['sourceDataUpdateDate'] + '","' + meta["buildUpdateDate"] + '","'


        metaLine = metaLine + isoMeta["Continent"].values[0] + '","' + isoMeta["UNSDG-region"].values[0] + '","'
        metaLine = metaLine + isoMeta["UNSDG-subregion"].values[0] + '","'
        metaLine = metaLine + isoMeta["worldBankIncomeGroup"].values[0] + '","'

        metaLine = metaLine + "https://www.geoboundaries.org/api/gbID/" + meta['boundaryID'] + '","'

        print("Preparing to calculate geometry statistics.")
        #Calculate geometry statistics
        #We'll use the geoJSON here, as the statistics (i.e., vertices) will be most comparable
        #to other cases.
        #Build geoJSON link
        gJLink = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/" + releaseType + "/" + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + ".geojson"
        print(gJLink)

        if not gJLink:
            print('Error: Missing GeoJSON file!')
            continue

        try:
            #text = urllib.request.urlopen(gJLink).read().decode('utf8')
            #fobj = io.StringIO(text)
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


        except:
            print("An error occured while trying to load the file.")
            metaLine = metaLine + "Error" + "Error" + '","' + "Error" + '","' + "Error" + '","' + "Error" + '","'

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

