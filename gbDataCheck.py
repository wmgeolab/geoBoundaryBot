import os
import sys
import zipfile
import subprocess
import pandas as pd
import geopandas
import gbHelpers
from matplotlib import pyplot as plt
from shapely.geometry import shape



def geometryCheck(ws):
    if(len(ws["zips"]) > 0):
        gbHelpers.logWrite(ws["checkType"],  "Modified zip files found.  Checking shape data validity.")
        gbHelpers.logWrite(ws["checkType"],  "")
        
        for z in ws["zips"]:
            gbHelpers.checkRetrieveLFSFiles(z, ws['working'])

            ws["zipTotal"] = ws["zipTotal"] + 1
            req = {}
            opt = {}

            opt["bndName"] = 0
            opt["nameExample"] = ""
            opt["nameCount"] = 0
            opt["bndISO"] = 0
            opt["isoExample"] = ""
            opt["isoCount"] = 0
            opt["topology"] = 1
            
            req["proj"] = 0


            checkFail = 0

            #Checks begin....
            gbHelpers.logWrite(ws["checkType"],  "Data Check (" + str(ws["zipTotal"]) + " of " + str(len(ws["zips"])) + "): " + z)
            bZip = zipfile.ZipFile(ws["working"] + "/" + z)

            #Extract the zipfiles contents
            bZip.extractall("tmp/")

            geojson = list(filter(lambda x: x[-4:] == '.geojson', bZip.namelist()))
            shp = list(filter(lambda x: x[-4:] == '.shp', bZip.namelist()))
            allShps = geojson + shp

            if(len(allShps) == 1):
                if(len(shp) == 1):
                    gbHelpers.logWrite(ws["checkType"],  "Shapefile (*.shp) found. Attempting to load.")
                    try:
                        dta = geopandas.read_file("tmp/" + shp[0])
                    except:
                        gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: The shape file provided failed to load. Make sure all required files are included (i.e., *.shx).")
                        checkFail = 1
                        break

                if(len(geojson) == 1):
                    gbHelpers.logWrite(ws["checkType"],  "geoJSON (*.geojson) found. Attempting to load.")
                    try:
                        dta = geopandas.read_file(geojson[0])
                    except:
                        gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: The geoJSON provided failed to load.")
                        checkFail = 1
                
                
                nameC = set(['Name', 'name', 'NAME', 'shapeName', 'shapename', 'SHAPENAME']) 
                nameCol = list(nameC & set(dta.columns))
                if(len(nameCol) == 1):
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "Column for name detected: " + str(nameCol[0]))
                    nameExample = dta[str(nameCol[0])][0]
                    nameValues = (dta[dta[str(nameCol[0])].str.contains('.*', regex=True)][str(nameCol[0])]).count()
                    gbHelpers.logWrite(ws["checkType"],  "Total number of names detected: " + str(nameValues))
                    gbHelpers.logWrite(ws["checkType"],  "Example of first name detected: " + str(nameExample))
                    opt["bndName"] = 1
                    opt["nameExample"] = nameExample
                    opt["nameCount"] = nameValues
                else:
                    gbHelpers.logWrite(ws["checkType"],  "WARN: No column for boundary names found.  This is not required.")

                nameC = set(['ISO', 'ISO_code', 'ISO_Code', 'iso', 'shapeISO', 'shapeiso', 'shape_iso']) 
                nameCol = list(nameC & set(dta.columns))
                if(len(nameCol) == 1):
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "Column for boundary ISO detected: " + str(nameCol[0]))
                    nameExample = dta[str(nameCol[0])][0]
                    nameValues = (dta[dta[str(nameCol[0])].str.contains('.*', regex=True)][str(nameCol[0])]).count()
                    gbHelpers.logWrite(ws["checkType"],  "Total number of boundary ISOs detected: " + str(nameValues))
                    gbHelpers.logWrite(ws["checkType"],  "Example of first boundary ISO detected: " + str(nameExample))
                    opt["bndISO"] = 1
                    opt["isoExample"] = nameExample
                    opt["isoCount"] = nameValues

                    if(len(opt["isoExample"]) < 3):
                        gbHelpers.logWrite(ws["checkType"],  "WARN: While a boundary ISO code column exists with data, the data appears to be invalid and would not be used in a release.  Please ensure the ISO codes follow ISO 3166-2, or the appropriate equivalent standard.")

                else:
                    gbHelpers.logWrite(ws["checkType"],  "WARN: No column for boundary ISOs found.  This is not required.")
                
                #Create a map visualization.  Skip for full builds, as the preview images will be
                #created in the build script for those.
                #fig, axes = plt.subplots(nrows=1, ncols=1)
                if(ws["checkType"] != "gbAuthoritative" and ws["checkType"] != "gbOpen" and ws["checkType"] != "gbHumanitarian"):
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
                            gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: At least one polygon is invalid and cannot be corrected.")
                            gbHelpers.logWrite(ws["checkType"],  "Here is what we know: " + str(row))
                            opt["topology"] = 0
                        else:
                            warnBuffer = 1
                            gbHelpers.logWrite(ws["checkType"],  "WARN: At least one polygon is invalid; automatically correcting with shapely buffer(0) clears this error, but it needs to be visually examined.")
                            gbHelpers.logWrite(ws["checkType"],  "Here is what we know: " + str(row))
                            opt["topology"] = 0

                if(validBounds == 1):
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "All shape geometries are within valid bounds.")
                else:
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: At least one geometry had bounds indicating it existed off the planet earth.  This is generally indicative of a projection error.")
                    checkFail = 1

                if(validGeom == 1):
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "All shape geometries have valid topology.")

                if(warnBuffer == 1):
                    gbHelpers.logWrite(ws["checkType"],  "")
                    gbHelpers.logWrite(ws["checkType"],  "WARN: At least one polygon was invalid, but could be cleared by shapely buffer(0).  It needs to be visually examined when possible.")


                if(dta.crs == "epsg:4326"):
                    gbHelpers.logWrite(ws["checkType"],  "Projection confirmed as " + str(dta.crs))
                    req["proj"] = 1
                else:
                    gbHelpers.logWrite(ws["checkType"],  "The projection must be EPSG 4326.  The file proposed has a projection of: " + str(dta.crs))
                    checkFail = 1
                    

            if(len(allShps) == 0):
                gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: No *.shp or *.geojson found for " + z)
                checkFail = 1
                
            if(len(allShps) > 1):
                gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: More than one geometry file (*.shp, *.geojson) was found for " + z)
                checkFail = 1
                        

            gbHelpers.logWrite(ws["checkType"],  "")
            gbHelpers.logWrite(ws["checkType"],  "Data checks complete for " + z)
            gbHelpers.logWrite(ws["checkType"],  "")
            gbHelpers.logWrite(ws["checkType"],  "----------------------------")
            gbHelpers.logWrite(ws["checkType"],  "      OPTIONAL TESTS        ")
            gbHelpers.logWrite(ws["checkType"],  "----------------------------")
            for i in opt:
                if(opt[i] == 1 or len(str(opt[i]))>1 or isinstance(opt[i], str) or opt[i]>0):
                    gbHelpers.logWrite(ws["checkType"],  '%-20s%-12s' % (i, "PASSED"))
                else:
                    gbHelpers.logWrite(ws["checkType"],  '%-20s%-12s' % (i, "FAILED"))
            gbHelpers.logWrite(ws["checkType"],  "")
            gbHelpers.logWrite(ws["checkType"],  "----------------------------")
            gbHelpers.logWrite(ws["checkType"],  "      REQUIRED TESTS        ")
            gbHelpers.logWrite(ws["checkType"],  "----------------------------")
            for i in req:
                if(req[i] == 1 or len(str(req[i]))>1 or isinstance(req[i], str) or req[i]>0):
                    gbHelpers.logWrite(ws["checkType"],  '%-20s%-12s' % (i, "PASSED"))
                else:
                    gbHelpers.logWrite(ws["checkType"],  '%-20s%-12s' % (i, "FAILED"))
            gbHelpers.logWrite(ws["checkType"],  "==========================")
                
                
            
            
            
            if(checkFail == 1):
                ws["zipFailures"] = ws["zipFailures"] + 1
                
            else:
                ws["zipSuccess"] = ws["zipSuccess"] + 1
                gbHelpers.logWrite(ws["checkType"],  "Data checks passed for " + z)
                

        gbHelpers.logWrite(ws["checkType"],  "")
        gbHelpers.logWrite(ws["checkType"],  "====================")
        gbHelpers.logWrite(ws["checkType"],  "All data checks complete.")
        gbHelpers.logWrite(ws["checkType"],  "Successes: " + str(ws["zipSuccess"]))
        gbHelpers.logWrite(ws["checkType"],  "Failures: " + str(ws["zipFailures"]))
        
        if(ws["zipFailures"] > 0):
            gbHelpers.logWrite(ws["checkType"], "CRITICAL ERROR: At least one data check failed; check the log to see what's wrong.")
            gbHelpers.gbEnvVars("RESULT", "A geometry or data check failed for the file you submitted - take a look at the logs to see what happened.", "w")
        else:
            gbHelpers.logWrite(ws["checkType"], "All tests passed.")
            gbHelpers.gbEnvVars("RESULT", "PASSED", "w")
        
        #Return of the last element for overall build
        return [opt, req, ws["zipSuccess"]]

    else:
        gbHelpers.logWrite(ws["checkType"],  "CRITICAL ERROR: No modified zip files found.")
        gbHelpers.gbEnvVars("RESULT", "Looks like you didn't submit a zip file.", "w")

if __name__ == "__main__":
    ws = gbHelpers.initiateWorkspace("geometryDataChecks")
    geometryCheck(ws)