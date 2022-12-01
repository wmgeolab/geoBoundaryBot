import zipfile
import hashlib 
import os
import geopandas as gpd
import datetime
from shapely.validation import explain_validity
from shapely.geometry import shape
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
import time

class builder:
    def __init__(self, ISO, ADM, product, basePath, logPath, tmpPath, validISO, validLicense):
        self.ISO = ISO
        self.ADM = ADM
        self.product = product
        self.basePath = basePath
        self.logPath = logPath
        self.tmpPath = tmpPath
        self.sourcePath = self.basePath + "sourceData/" + self.product + "/" + self.ISO + "_" + self.ADM + ".zip"
        self.zipExtractPath = self.tmpPath + self.product + "/" + self.ISO + "_" + self.ADM + "/"
        self.validISO = validISO
        self.validLicense = [x.lower().strip() for x in validLicense]        

        #Data Validity Checks
        self.existFail = 1
        self.metaExistsFail = 1
        self.dataExtractFail = 1
        self.hashCalcFail = 1
        self.dataLoadFail = 1

        #Meta validity checks
        #Note canonical and license image are new reqs in 5.0.
        self.metaReq = {}
        self.metaReq["year"] = "NONE"
        self.metaReq["bType"] = "NONE"
        self.metaReq["iso"] = "NONE"
        self.metaReq["source"] = "NONE"
        self.metaReq["releaseType"] = "NONE"
        self.metaReq["license"] = "NONE"
        self.metaReq["licenseSource"] = "NONE"
        self.metaReq["dataSource"] = "NONE"
        self.metaReq["canonical"] = ""
        self.metaReq["licenseImage"] = "NONE"
        self.metaReq["licenseNotes"] = ""
        self.metaReq["other notes"] = ""

        #Geometry / Attribute Table Validity Checks
        self.geomReq = {}
        self.geomReq["projection"] = "NONE"
        self.geomReq["names"] = "NONE"
        self.geomReq["iso"] = "NONE"
        self.geomReq["bounds"] = "NONE"
        self.geomReq["valid"] = "NONE"

        #Library for metadata
        self.metaData = {}
    
    def logger(self, type, message):
        with open(self.logPath + str(self.ISO)+str(self.ADM)+str(self.product)+".log", "a") as f:
            f.write(str(type) + ": " + str(message) + "\n")
    
    def checkExistence(self):
        if os.path.exists(self.sourcePath):
            self.existFail = 0
            self.logger("INFO", "File Exists: " + str(self.sourcePath))
        else:
            self.logger("CRITICAL","File does not Exist: " + str(self.sourcePath))

    def metaLoad(self):
        try:
            with zipfile.ZipFile(self.sourcePath) as zF:
                self.metaData = zF.read('meta.txt').decode("utf-8")
                self.metaExistsFail = 0
        except Exception as e:
            self.logger("CRITICAL","Metadata failed to load: " + str(e))

    def unzip(self):
        try:
            sourceZip = zipfile.ZipFile(self.sourcePath)
            sourceZip.extractall(self.zipExtractPath )
            if(os.path.exists(self.zipExtractPath  + "__MACOSX")):
                shutil.rmtree(self.zipExtractPath  + "__MACOSX")
            self.dataExtractFail = 0
        except Exception as e:
            self.logger("CRITICAL","Zipfile extraction failed: " + str(e))

    def dataLoad(self):
            self.unzip()
            sourceZip = zipfile.ZipFile(self.sourcePath)

            geojson = list(filter(lambda x: x[-8:] == '.geojson', sourceZip.namelist()))
            shp = list(filter(lambda x: x[-4:] == '.shp', sourceZip.namelist()))

            geojson = [x for x in geojson if not x.__contains__("MACOS")]
            shp = [x for x in shp if not x.__contains__("MACOS")]

            allShps = geojson + shp

            if(len(allShps) == 1):
                if(len(shp) == 1):
                    self.logger("INFO", "Shapefile (*.shp) found. Attempting to load.")
                    try:
                        self.geomDta = gpd.read_file(self.zipExtractPath + shp[0])
                        self.dataLoadFail = 0
                    except Exception as e:
                        self.logger("CRITICAL", "The shape file provided failed to load. Make sure all required files are included (i.e., *.shx). Here is what I know: " + str(e))
                        
                if(len(geojson) == 1):
                    self.logger("INFO", "geoJSON (*.geojson) found. Attempting to load.")
                    try:
                        self.geomDta = gpd.read_file(self.zipExtractPath + geojson[0])
                        self.dataLoadFail = 0
                    except Exception as e:
                        self.logger("CRITICAL", "The geojson provided failed to load. Here is what I know: " + str(e))
            
            else:
                self.logger("CRITICAL", "There was more than one geometry file (shapefile or geojson) present in the zipfile.")

    def hashCalc(self):
        m = hashlib.sha256()
        chunkSize = 8192
        with open(self.basePath + "sourceData/" + self.product + "/" + self.ISO + "_" + self.ADM + ".zip", 'rb') as zF:
            while True:
                chunk = zF.read(chunkSize)
                if(len(chunk)):
                    m.update(chunk)
                else:
                    break
                    
            self.metaHash = str(int(m.hexdigest(), 16) % 10**8)
            self.logger("INFO", "Hash Calculated: " + str(self.metaHash))
            self.hashCalcFail = 0
    
    def checkSourceValidity(self):
        self.checkExistence()
        if(self.existFail == 1):
            return("INFO: Source file does not exist for this boundary.")

        self.unzip()
        if(self.unzip == 1):
            return("ERROR: The zipfile in the source directory failed to extract correctly.")

        self.metaLoad()
        if(self.metaExistsFail == 1):
            return("ERROR: There is no meta.txt in the source zipfile for this boundary.")
        
        self.dataLoad()
        if(self.dataLoadFail == 1):
            return("ERROR: The geometry file (shape or geojson) in the zipfile failed to load into geopandas.")
        
        return("SUCCESS: Source zipfile is valid.")
    
    def checkBuildTabularMetaData(self):
        self.metaExistsFail = 1
        self.metaLoad()

        if(self.metaExistsFail == 1):
            return("ERROR: There is no meta.txt in the source zipfile for this boundary.")

        self.logger("INFO", "Beginning meta.txt validity checks.")


        for m in self.metaData.splitlines():
            try:
                e = m.decode("utf-8").split(":")
                if(len(e) > 2):
                    e[1] = e[1] + e[2]
                key = e[0].strip()
                val = e[1].strip()
            except:
                self.logger("WARN", "At least one line of meta.txt failed to be read correctly: " + str(m))
                key = "readError"
                val = "readError"
            
            if(("Year" in key) or ("year" in key)):
                #pre 4.0 legacy cleanup
                if(".0" in str(val)):
                    val = str(val)[:-2]
                try:
                    if "to" in val:
                        date1, date2 = val.split(" to ")
                        date1 = datetime.datetime.strptime(date1, "%d-%m-%Y")
                        date2 = datetime.datetime.strptime(date2, "%d-%m-%Y")
                        self.logger("INFO", "Valid date range " + str(val) + " detected.")
                        self.metaReq["year"] = str(val)
                    else:
                        year = int(float(val))
                        if( (year > 1950) and (year <= datetime.datetime.now().year)):
                            self.logger("INFO", "Valid year " + str(year) + " detected.")
                            self.metaReq["year"] = str(val)
                        else:
                            self.logger("CRITICAL", "The year in the meta.txt file is invalid (expected value is between 1950 and present): " + str(year))
                            self.metaReq["year"] = "ERROR: The year provided in the metadata was invalid (Not numerically what we expected): " + str(year)
                except Exception as e:
                    self.logger("CRITICAL", "The year provided in the metadata " + str(val) + " was invalid. This is what I know:" + str(e))
                    self.metaReq["year"] = "ERROR: The year provided in the metadata was invalid (Exception in log)."

            
            if("boundary type" in key.lower() and "name" not in key.lower()):
                try:
                    validTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
                    if(val.upper().replace(" ","") in validTypes):
                        self.logger("INFO", "Valid Boundary Type detected: " + str(val) +".")
                        self.metaReq["bType"] = val
                    else:
                        self.logger("CRITICAL", "The boundary type in the meta.txt file is invalid: " + str(val))
                        self.metaReq["bType"] = "ERROR: The boundary type in the meta.txt file in invalid."
                except Exception as e:
                    self.logger("CRITICAL", "The boundary type in the meta.txt file was invalid. This is what I know:" + str(e))
                    self.metaReq["bType"] = "ERROR: The boundary type in the meta.txt file was invalid (Exception in log)."


            if("iso" in key.lower().strip()):
                if(len(val) != 3):
                    self.logger("CRITICAL", "ISO is invalid - we expect a 3-character ISO code following ISO-3166-1 (Alpha 3).")
                    self.metaReq["iso"] = "ERROR: ISO is invalid - we expect a 3-character ISO code following ISO-3166-1 (Alpha 3)."
                elif(val not in self.validISO):
                    self.logger("CRITICAL", "ISO is not on our list of valid ISO-3 codes.  See https://github.com/wmgeolab/geoBoundaryBot/blob/main/dta/iso_3166_1_alpha_3.csv for all valid codes this script checks against.")
                    self.metaReq["iso"] = "ERROR: ISO is not on our list of valid ISO-3 codes."
                else:
                    self.logger("INFO", "Valid ISO detected: " + str(val))
                    self.metaReq["iso"] = val


            if("canonical" in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.logger("INFO", "Canonical name detected: " + str(val))
                        self.metaReq["canonical"] = val
                else:
                    self.logger("WARN", "No canonical name detected.")
                    self.metaReq["canonical"] = ""


            if("source" in key.lower() and "license" not in key.lower() and "data" not in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.logger("INFO", "Source detected: " + str(val))
                        if(self.metaReq["source"] == "NONE"):
                            self.metaReq["source"] = val
                        else:
                            self.metaReq["source"] = self.metaReq["source"] + ", " + val


            if("release type" in key.lower()):
                if (val.lower() not in ["geoboundaries", "gbauthoritative", "gbhumanitarian", "gbopen", "un_salb", "un_ocha"]):
                    self.logger("CRITICAL", "Invalid release type detected: " + str(val))
                    self.metaReq["releaseType"] = "ERROR: Invalid release type detected."
                else:
                    #Legacy fixes for gb 4.0 and earlier.
                    if(val.lower() == "gbauthoritative"):
                        val = "UN_SALB"
                    if(val.lower() == "gbhumanitarian"):
                        val = "UN_OCHA"
                    if(val.lower() == "gbopen"):
                        val = "geoBoundaries"
                    
                    if(val.lower() not in self.sourcePath.lower()):
                        self.logger("CRITICAL", "ERROR: Mismatch between release type and the folder the source zip file was located in.")
                        self.metaReq["releaseType"] = "ERROR: Mismatch between release type and the folder the source zip file was located in: " + str(val.lower()) + " | " + str(self.sourcePath.lower())
                    else:
                        self.metaReq["releaseType"]  = val.lower().strip()



            if("license" == key.lower()):
                if(val.lower().strip() not in self.validLicense):
                    self.logger("CRITICAL", "Invalid license detected: " + str(val))
                    self.metaReq["license"] = "ERROR: Invalid license detected: " + str(val)
                else:
                    self.metaReq["license"] = val
                    self.logger("INFO", "Valid license type detected: " + str(val))



            if("license notes" in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.logger("INFO", "License notes detected: " + str(val))
                        self.metaReq["licenseNotes"] = str(val)
                else:
                    self.logger("INFO", "No license notes detected.")
                    self.metaReq["licenseNotes"] = ""



            if("license source" in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.logger("INFO", "License source detected: " + str(val))
                        self.metaReq["licenseSource"] = val

                        #Check for a png image of the license source.
                        #Any png or jpg with the name "license" is accepted.
                        licPic = 0
                        try:
                            with zipfile.ZipFile(wself.sourcePath) as zFb:
                                licPic = zFb.read('license.png')
                        except:
                            pass

                        try:
                            with zipfile.ZipFile(self.sourcePath) as zFb:
                                licPic = zFb.read('license.jpg')
                        except:
                            pass

                        if(licPic != 0):
                             self.logger("INFO", "License image found.")
                             self.metaReq["licenseImage"] = "Image Available"
                        else:
                            self.logger("WARN", "No license image found. We check for license.png and license.jpg.")
                            self.metaReq["licenseImage"] = "None Available"
                    else:
                        self.logger("CRITICAL", "No license source detected.")
                        self.metaReq["licenseImage"] = "ERROR: No license source detected."
                else:
                    self.logger("CRITICAL", "No license source detected.")
                    self.metaReq["licenseImage"] = "ERROR: No license source detected."


            if("link to source data" in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.metaReq["dataSource"] = val
                        self.logger("INFO", "Data Source Found: " + str(val))
                                        
                    else:
                        self.metaReq["dataSource"] = "ERROR: No link to source data found."
                        self.logger("CRITICAL", "ERROR: No link to source data found.")

                else:
                    self.metaReq["dataSource"] = "ERROR: No link to source data found."
                    self.logger("CRITICAL", "ERROR: No link to source data found.")


            if("other notes" in key.lower()):
                if(len(val.replace(" ","")) > 0):
                    if(val.lower() not in ["na", "nan", "null"]):
                        self.logger("INFO", "Other notes detected: " + val)
                        self.metaReq["otherNotes"] = val
                else:
                    self.logger("WARN", "No other notes detected.  This field is optional.")

        allValid = 1
        retMes = ""
        for d in self.metaReq:
            if(self.metaReq[d] == "NONE"):
                self.logger("CRITICAL",str(d) + "FAILED - NO METADATA PRESENT.")
                retMes = retMes + " | Missing data for " + str(d) + "."
                allValid = 0
            
            if("ERROR" in self.metaReq[d]):
                self.logger("CRITICAL",str(d) + "FAILED - ERROR IN DATA." + str(self.metaReq[d]))
                retMes = retMes + " | " + str(self.metaReq[d])
                allValid = 0
        
        if(allValid == 1):
            self.logger("INFO", "All metadata checks passed, commencing build stage.")
            self.hashCalc()
            self.metaData["boundaryID"] = str(str(self.ISO) + "-" + str(self.ADM) + "-" + str(self.metaHash))
            self.metaData["boundaryISO"] = self.ISO
            self.metaData["boundaryYear"] = self.metaReq["year"]
            self.metaData["boundaryType"] = self.metaReq["bType"]
            self.metaData["boundarySource"] = self.metaReq["source"]
            self.metaData["boundaryCanonical"] = self.metaReq["canonical"]
            self.metaData["boundaryLicense"] = self.metaReq["license"]
            self.metaData["licenseDetail"] = self.metaReq["licenseNotes"]
            self.metaData["licenseSource"] = self.metaReq["licenseSource"]
            self.metaData["boundarySourceURL"] = self.metaReq["dataSource"]
            self.metaData["sourceDataUpdateDate"] = os.popen("git log -1 --format=%cd -p -- " + self.sourcePath).read().split("-")[0].strip()
            self.metaData["buildDate"] = time.strftime('%b %d, %Y')
            return("Metadata checks successful, metadata built in self.metaData.")
        
        else:
            self.logger("CRITICAL", "At least one metadata check failed, build halted..")
            return(retMes)
    
    def checkBuildGeometryFiles(self):
        self.dataLoad()
        
        nameC = set(['Name', 'name', 'NAME', 'shapeName', 'shapename', 'SHAPENAME']) 
        nameCol = list(nameC & set(self.geomDta.columns))
        if(len(nameCol) == 1):
            self.logger("INFO", "Column for name detected: " + str(nameCol[0]))
            try:
                nameExample = self.geomDta[str(nameCol[0])][0]
                nameValues = (self.geomDta[self.geomDta[str(nameCol[0])].str.contains('.*', regex=True)][str(nameCol[0])]).count()
                self.geomReq["names"] = str(nameCol[0])
                self.logger("INFO", "Names: " + str(nameValues) + " | Example: " + str(nameExample))
            except Exception as e:
                self.logger("WARN", "No name values were found, even though a column was present.")
                self.geomReq["names"] = str(nameCol[0])

        nameIC = set(['ISO', 'ISO_code', 'ISO_Code', 'iso', 'shapeISO', 'shapeiso', 'shape_iso']) 
        nameICol = list(nameIC & set(self.geomDta.columns))
        if(len(nameICol) == 1):
            self.logger("INFO", "Column for ISO detected: " + str(nameICol[0]))
            try:
                nameExample = self.geomDta[str(nameICol[0])][0]
                nameValues = (self.geomDta[self.geomDta[str(nameICol[0])].str.contains('.*', regex=True)][str(nameICol[0])]).count()
                self.geomReq["iso"] = str(nameICol[0])
                self.logger("INFO", "ISOs: " + str(nameValues) + " | Example: " + str(nameExample))
            except Exception as e:
                self.logger("WARN", "No ISO values were found, even though a column was present.")
                self.geomReq["iso"] = str(nameICol[0])
        else:
            self.logger("WARN","No column for boundary ISOs found.  This is not required.")

        for index, row in self.geomDta.iterrows():
            xmin = row["geometry"].bounds[0]
            ymin = row["geometry"].bounds[1]
            xmax = row["geometry"].bounds[2]
            ymax = row["geometry"].bounds[3]
            tol = 1e-5
            valid = ((xmin >= -180-tol) and (xmax <= 180+tol) and (ymin >= -90-tol) and (ymax <= 90+tol))
            if not valid:
                self.geomReq["bounds"] = "ERROR: At least one geometry seems to extend past the boundaries of the earth: " + str(explain_validity(row["geometry"]))
                self.logger("CRITICAL", "ERROR: This geometry seems to extend past the boundaries of the earth: " + str(explain_validity(row["geometry"])))
                         
            if(not row["geometry"].is_valid):
                self.logger("WARN", "Something is wrong with this geometry, but we might be able to fix it with a buffer: " + str(explain_validity(row["geometry"])))
                if(not row["geometry"].buffer(0).is_valid):
                    self.geomReq["valid"] = "ERROR: Something is wrong with this geometry, and we can't fix it: " + str(explain_validity(row["geometry"]))
                    self.logger("CRITICAL","ERROR: Something is wrong with this geometry, and we can't fix it: " + str(explain_validity(row["geometry"])))
                else:
                    self.logger("WARN","A geometry error was corrected with buffer=0 in shapely.")

        if("ERROR" not in self.geomReq["bounds"]):
            self.logger("INFO","All geometries are within valid bounds.")
            self.geomReq["bounds"] = "All geometries within valid bounds."

        if("ERROR" not in self.geomReq["valid"]):
            self.logger("INFO", "All geometries were topologically valid.")
            self.geomReq["valid"] = "All geometries topologically valid."
        
        try:
            if(self.geomDta.crs == "epsg:4326"):
                self.logger("INFO",  "Projection confirmed as " + str(self.geomDta.crs))
                self.geomReq["projection"] = "Projection confirmed as " + str(self.geomDta.crs)
            else:
                self.logger("CRITICAL",  "The projection must be EPSG 4326.  The file proposed has a projection of: " + str(self.geomDta.crs))
                self.geomReq["projection"] = "ERROR: The projection must be EPSG 4326.  The file proposed has a projection of: " + str(self.geomDta.crs)
        except:
            self.logger("CRITICAL",  "The projection must be EPSG 4326.")
            self.geomReq["projection"] = "ERROR: The projection must be EPSG 4326."

        allValid = 1
        retMes = ""
        for d in self.geomReq:
            if(self.geomReq[d] == "NONE"):
                self.logger("CRITICAL",str(d) + " FAILED CHECK.")
                retMes = retMes + " | Failed Check for " + str(d) + "."
                allValid = 0
            
            if("ERROR" in self.geomReq[d]):
                self.logger("CRITICAL",str(d) + " FAILED - ERROR IN DATA." + str(self.geomReq[d]))
                retMes = retMes + " | " + str(self.geomReq[d])
                allValid = 0
        
        if(allValid == 1):
            self.logger("INFO", "All geometry checks passed, commencing build.")
            try:
                #Cast to multipolygons
                self.geomDta["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in self.geomDta["geometry"]]

                #Standardize name and ISO columns, if they exist; otherwise create blank columns.
                if(self.geomReq["iso"] != "NONE"):
                    self.geomDta = self.geomDta.rename(columns={self.geomReq["iso"]:"shapeISO"})
                else:
                    self.geomDta["shapeISO"] = ""
                if(self.geomReq["names"] != "NONE"):
                    self.geomDta = self.geomDta.rename(columns={self.geomReq["names"]:"shapeName"})
                else:
                    self.geomDta["shapeName"] = ""
                
                #Build shape IDs
                self.hashCalc()
                def geomID(geom, metaHash = self.metaHash):
                    hashVal = int(hashlib.sha256(str(geom["geometry"]).encode(encoding='UTF-8')).hexdigest(), 16) % 10**14
                    return(str(metaHash) + "B" + str(hashVal))

                self.geomDta["shapeID"] = self.geomDta.apply(lambda row: geomID(row), axis=1)
                self.logger("INFO", "ADMISO assignment")
                self.geomDta[["shapeGroup"]] = self.ISO
                self.geomDta[["shapeType"]] = self.ADM

                #Cleanup by removing columns not on our list
                keepCols = ["shapeGroup","shapeID","shapeType","shapeISO","geometry"]
                self.logger("INFO", "Keeping Cols")
                self.geomDta = self.geomDta.drop(columns=[c for c in self.geomDta if c not in keepCols])
                return("Geometry checks and build successful.")

            except Exception as e:
                self.logger("CRITICAL","Building the geometries failed: " + str(e))
                return("ERROR: Geometry build failed, check the log.")
            
        else:
            self.logger("CRITICAL", "At least one geometry check failed.")
            return(retMes)
    def constructFiles():
        #Create temp working folder
        tmpFold = self.tmpPath + self.ISO + self.ADM + self.product + "/"
        if not os.path.exists(tmpFold):
            os.makedir(tmpFold)
        
        #Save intermediary geoJSON
        self.geomDta.to_file(self.tmpPath + self.ISO + self.ADM + self.product + ".geoJSON", driver="GeoJSON")

        
        