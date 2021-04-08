import os
import re 
import json
import pandas as pd
import geopandas

#Initialize workspace
ws = {}
try:
    ws['working'] = os.environ['GITHUB_WORKSPACE']
    ws['logPath'] = os.path.expanduser("~") + "/tmp/log.txt"
except:
    ws['working'] = "/home/dan/git/gbRelease"
    ws['logPath'] = os.path.expanduser("~") + "/tmp/log.txt"

#Load in the ISO lookup table
isoDetails = pd.read_csv(ws['working'] + "/geoBoundaryBot/dta/iso_3166_1_alpha_3.csv")


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
    f.write("boundaryID,Country,boundaryISO,boundaryYear,boundaryType,boundaryCanonical,boundarySource-1,boundarySource-2,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildUpdateDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,apiURL,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,\n")

with open(gbOpenCSV,'w+') as f:
    headerWriter(f)

with open(gbHumCSV,'w+') as f:
    headerWriter(f)

with open(gbAuthCSV,'w+') as f:
    headerWriter(f)

for (path, dirname, filenames) in os.walk(ws["working"] + "/releaseData/"):
    if("gbHumanitarian" in path):
        csvPath = gbHumCSV
    elif("gbOpen" in path):
        csvPath = gbOpenCSV
    elif("gbAuthoritative" in path):
        csvPath = gbAuthCSV
    else:
        continue
    
    metaSearch = [x for x in filenames if re.search('metaData.json', x)]
    if(len(metaSearch)==1):
        with open(path + "/" + metaSearch[0], "r") as j:
            meta = json.load(j)
        
        isoMeta = isoDetails[isoDetails["Alpha-3code"] == meta['boundaryISO']]
        #Build the metadata
        metaLine = '"' + meta['boundaryID'] + '","' + isoMeta["Country"].values[0] + '","' + meta['boundaryISO'] + '","' + meta['boundaryYear'] + '","' + meta["boundaryType"] + '","'

        if("boundaryCanonical" in meta):
            if(len(meta["boundaryCanonical"])>0):
                metaLine = metaLine + meta["boundaryCanonical"] + '","'
            else:
                metaLine = metaLine + 'Unkown","'
        else:
            metaLine = metaLine + 'Unkown","'

        metaLine = metaLine + meta['boundarySource-1'] + '","' + meta['boundarySource-2'] + '","' + meta['boundaryLicense'] + '","' + meta['licenseDetail'] + '","' + meta['licenseSource'] + '","'
        metaLine = metaLine + meta['boundarySourceURL'] + '","' + meta['sourceDataUpdateDate'] + '","' + meta["buildUpdateDate"] + '","'
        
        
        metaLine = metaLine + isoMeta["Continent"].values[0] + '","' + isoMeta["UNSDG-region"].values[0] + '","'
        metaLine = metaLine + isoMeta["UNSDG-subregion"].values[0] + '","' 
        metaLine = metaLine + isoMeta["worldBankIncomeGroup"].values[0] + '","'

        metaLine = metaLine + "https://www.geoboundaries.org/gbRequest.html?gbID=" + meta['boundaryID'] + '","'

        #Calculate geometry statistics
        #We'll use the geoJSON here, as the statistics (i.e., vertices) will be most comparable
        #to other cases.
        geojsonSearch = [x for x in filenames if re.search('.geojson', x)]
        with open(path + "/" + geojsonSearch[0], "r") as g:
            geom = geopandas.read_file(g)
        
        admCount = len(geom)
        
        vertices=[]
        for i, row in geom.iterrows():
            n = 0
            print(row.geometry.type)
            if(row.geometry.type.startswith("Multi")):
                for seg in row.geometry:
                    n += len(seg.exterior.coords)
            else:
                n = len(row.geometry.exterior.coords)
            
            vertices.append(n) ###
        
        metaLine = metaLine + str(admCount) + '","' + str(round(sum(vertices)/len(vertices),0)) + '","' + str(min(vertices)) + '","' + str(max(vertices)) + '","'

        #Perimeter Using WGS 84 / World Equidistant Cylindrical (EPSG 4087)
        lengthGeom = geom.copy()
        lengthGeom = lengthGeom.to_crs(epsg=4087)
        lengthGeom["length"] = lengthGeom["geometry"].length / 1000 #km
        
        metaLine = metaLine + str(lengthGeom["length"].mean()) + '","' + str(lengthGeom["length"].min()) + '","' + str(lengthGeom["length"].max()) + '","'

        #Area #mean min max Using WGS 84 / EASE-GRID 2 (EPSG 6933)
        areaGeom = geom.copy()
        areaGeom = areaGeom.to_crs(epsg=6933)
        areaGeom["area"] = areaGeom['geometry'].area / 10**6 #sqkm

        metaLine = metaLine + str(areaGeom['area'].mean()) + '","' + str(areaGeom['area'].min()) + '","' + str(areaGeom['area'].max()) + '","'
        #Cleanup
        metaLine = metaLine + '"\n'
        metaLine = metaLine.replace("nan","")

        with open(csvPath,'a') as f:
            f.write(metaLine)
    