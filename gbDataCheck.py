import os
import sys
import zipfile
import subprocess
import pandas as pd
import geopandas
import gbHelpers
from matplotlib import pyplot as plt
from shapely.geometry import shape

checkType = "geometryDataCheck"
ws = gbHelpers.initiateWorkspace(checkType)

zipFailures = 0
zipSuccess = 0
zipTotal = 0
anyFail = 0

if(len(ws["zips"]) > 0):
    gbHelpers.logWrite(checkType,  "Modified zip files found.  Checking shape data validity.")
    gbHelpers.logWrite(checkType,  "")
    
    for z in ws["zips"]:
        gbHelpers.checkRetrieveLFSFiles(z, ws['working'])

        zipTotal = zipTotal + 1
        req = {}
        opt = {}

        opt["bndName"] = 0
        opt["nameExample"] = ""
        opt["nameCount"] = 0
        opt["bndISO"] = 0
        opt["isoExample"] = ""
        opt["isoCount"] = 0
        
        req["topology"] = 0
        req["proj"] = 0


        checkFail = 0

        #Checks begin....
        gbHelpers.logWrite(checkType,  "Data Check (" + str(zipTotal) + " of " + str(len(ws["zips"])) + "): " + z)
        bZip = zipfile.ZipFile(ws["working"] + "/" + z)

        #Extract the zipfiles contents
        bZip.extractall("tmp/")

        geojson = list(filter(lambda x: x[-4:] == '.geojson', bZip.namelist()))
        shp = list(filter(lambda x: x[-4:] == '.shp', bZip.namelist()))
        allShps = geojson + shp

        if(len(allShps) == 1):
            if(len(shp) == 1):
                gbHelpers.logWrite(checkType,  "Shapefile (*.shp) found. Attempting to load.")
                try:
                    dta = geopandas.read_file("tmp/" + shp[0])
                except:
                    gbHelpers.logWrite(checkType,  "CRITICAL ERROR: The shape file provided failed to load. Make sure all required files are included (i.e., *.shx).")
                    checkFail = 1
                    break

            if(len(geojson) == 1):
                gbHelpers.logWrite(checkType,  "geoJSON (*.geojson) found. Attempting to load.")
                try:
                    dta = geopandas.read_file(geojson[0])
                except:
                    gbHelpers.logWrite(checkType,  "CRITICAL ERROR: The geoJSON provided failed to load.")
                    checkFail = 1
            
            
            nameC = set(['Name', 'name', 'NAME', 'shapeName', 'shapename', 'SHAPENAME']) 
            nameCol = list(nameC & set(dta.columns))
            if(len(nameCol) == 1):
                gbHelpers.logWrite(checkType,  "")
                gbHelpers.logWrite(checkType,  "Column for name detected: " + str(nameCol[0]))
                nameExample = dta[str(nameCol[0])][0]
                nameValues = (dta[dta[str(nameCol[0])].str.contains('.*', regex=True)][str(nameCol[0])]).count()
                gbHelpers.logWrite(checkType,  "Total number of names detected: " + str(nameValues))
                gbHelpers.logWrite(checkType,  "Example of first name detected: " + str(nameExample))
                opt["bndName"] = 1
                opt["nameExample"] = nameExample
                opt["nameCount"] = nameValues
            else:
                gbHelpers.logWrite(checkType,  "WARN: No column for boundary names found.  This is not required.")

            nameC = set(['ISO', 'ISO_code', 'ISO_Code', 'iso', 'shapeISO', 'shapeiso', 'shape_iso']) 
            nameCol = list(nameC & set(dta.columns))
            if(len(nameCol) == 1):
                gbHelpers.logWrite(checkType,  "")
                gbHelpers.logWrite(checkType,  "Column for boundary ISO detected: " + str(nameCol[0]))
                nameExample = dta[str(nameCol[0])][0]
                nameValues = (dta[dta[str(nameCol[0])].str.contains('.*', regex=True)][str(nameCol[0])]).count()
                gbHelpers.logWrite(checkType,  "Total number of boundary ISOs detected: " + str(nameValues))
                gbHelpers.logWrite(checkType,  "Example of first boundary ISO detected: " + str(nameExample))
                opt["bndISO"] = 1
                opt["isoExample"] = nameExample
                opt["isoCount"] = nameValues

                if(len(opt["isoExample"]) < 3):
                    gbHelpers.logWrite(checkType,  "WARN: While a boundary ISO code column exists with data, the data appears to be invalid and would not be used in a release.  Please ensure the ISO codes follow ISO 3166-2, or the appropriate equivalent standard.")

            else:
                gbHelpers.logWrite(checkType,  "WARN: No column for boundary ISOs found.  This is not required.")
            
            #Create a map visualization.
            #fig, axes = plt.subplots(nrows=1, ncols=1)
            dta.boundary.plot()
            plt.savefig(os.path.expanduser("~") + "/tmp/preview.png")

            for index, row in dta.iterrows():
                validBounds = 1
                validGeom = 1
                warnBuffer = 0
                xmin = row["geometry"].bounds[0]
                ymin = row["geometry"].bounds[1]
                xmax = row["geometry"].bounds[2]
                ymax = row["geometry"].bounds[3]
                tol = 1e-12
                valid = ((xmin >= -180-tol) and (xmax <= 180+tol) and (ymin >= -90-tol) and (ymax <= 90+tol))
                if not valid:
                    checkFail = 1
                    validBounds = 0
                if(not row["geometry"].is_valid):
                    if(not row["geometry"].buffer(0).is_valid):
                        checkFail = 1
                        validGeom = 0
                        gbHelpers.logWrite(checkType,  "CRITICAL ERROR: At least one polygon is invalid and cannot be corrected.")
                        gbHelpers.logWrite(checkType,  "Here is what we know: " + str(row))
                    else:
                        warnBuffer = 1
                        checkFail = 1
                        gbHelpers.logWrite(checkType,  "CRITICAL ERROR: At least one polygon is invalid; automatically correcting with shapely buffer(0) clears this error, but it needs to be visually examined.")
                        gbHelpers.logWrite(checkType,  "Here is what we know: " + str(row))
                else:
                    req["topology"] = 1

            if(validBounds == 1):
                gbHelpers.logWrite(checkType,  "")
                gbHelpers.logWrite(checkType,  "All shape geometries are within valid bounds.")
            else:
                gbHelpers.logWrite(checkType,  "")
                gbHelpers.logWrite(checkType,  "CRITICAL ERROR: At least one geometry had bounds indicating it existed off the planet earth.  This is generally indicative of a projection error.")
            
            if(validGeom == 1):
                gbHelpers.logWrite(checkType,  "")
                gbHelpers.logWrite(checkType,  "All shape geometries have valid topology.")

            if(warnBuffer == 1):
                gbHelpers.logWrite(checkType,  "")
                checkFail = 1
                gbHelpers.logWrite(checkType,  "CRITICAL ERROR: At least one polygon was invalid, but could be cleared by shapely buffer(0).  It needs to be visually examined when possible.")


            if(dta.crs == "epsg:4326"):
                gbHelpers.logWrite(checkType,  "Projection confirmed as " + str(dta.crs))
                req["proj"] = 1
            else:
                gbHelpers.logWrite(checkType,  "The projection must be EPSG 4326.  The file proposed has a projection of: " + str(dta.crs))
                checkFail = 1
                

        if(len(allShps) == 0):
            gbHelpers.logWrite(checkType,  "CRITICAL ERROR: No *.shp or *.geojson found for " + z)
            checkFail = 1
            
        if(len(allShps) > 1):
            gbHelpers.logWrite(checkType,  "CRITICAL ERROR: More than one geometry file (*.shp, *.geojson) was found for " + z)
            checkFail = 1
                    

        gbHelpers.logWrite(checkType,  "")
        gbHelpers.logWrite(checkType,  "Data checks complete for " + z)
        gbHelpers.logWrite(checkType,  "")
        gbHelpers.logWrite(checkType,  "----------------------------")
        gbHelpers.logWrite(checkType,  "      OPTIONAL TESTS        ")
        gbHelpers.logWrite(checkType,  "----------------------------")
        for i in opt:
            if(opt[i] == 1 or len(str(opt[i]))>1 or isinstance(opt[i], str) or opt[i]>0):
                gbHelpers.logWrite(checkType,  '%-20s%-12s' % (i, "PASSED"))
            else:
                gbHelpers.logWrite(checkType,  '%-20s%-12s' % (i, "FAILED"))
        gbHelpers.logWrite(checkType,  "")
        gbHelpers.logWrite(checkType,  "----------------------------")
        gbHelpers.logWrite(checkType,  "      REQUIRED TESTS        ")
        gbHelpers.logWrite(checkType,  "----------------------------")
        for i in req:
            if(req[i] == 1 or len(str(req[i]))>1 or isinstance(req[i], str) or req[i]>0):
                gbHelpers.logWrite(checkType,  '%-20s%-12s' % (i, "PASSED"))
            else:
                gbHelpers.logWrite(checkType,  '%-20s%-12s' % (i, "FAILED"))
        gbHelpers.logWrite(checkType,  "==========================")
            
            
        
        
        
        if(checkFail == 1):
            zipFailures = zipFailures + 1
            anyFail = 1
            
        else:
            zipSuccess = zipSuccess + 1
            gbHelpers.logWrite(checkType,  "Data checks passed for " + z)

    gbHelpers.logWrite(checkType,  "")
    gbHelpers.logWrite(checkType,  "====================")
    gbHelpers.logWrite(checkType,  "All data checks complete.")
    gbHelpers.logWrite(checkType,  "Successes: " + str(zipSuccess))
    gbHelpers.logWrite(checkType,  "Failures: " + str(zipFailures))
    
    if(zipFailures > 0):
        gbHelpers.logWrite(checkType, "CRITICAL ERROR: At least one data check failed; check the log to see what's wrong.")
        gbHelpers.gbEnvVars("RESULT", "A geometry or data check failed for the file you submitted - take a look at the logs to see what happened.", "w")
    else:
        gbHelpers.gbEnvVars("RESULT", "PASSED", "w")

else:
    gbHelpers.logWrite(checkType,  "CRITICAL ERROR: No modified zip files found.")
    gbHelpers.gbEnvVars("RESULT", "Looks like you didn't submit a zip file.", "w")
