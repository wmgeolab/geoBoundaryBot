import os
import sys
import zipfile
import subprocess
import datetime
import gbHelpers

checkType = "metaChecks"
ws = gbHelpers.initiateWorkspace(checkType)

zipFailures = 0
zipSuccess = 0
zipTotal = 0
anyFail = 0

#Load ISOs for later checks
with open(ws["working"] + "/actions/dta/iso_3166_1_alpha_3.csv") as isoCsv:
    lines = isoCsv.readlines()

validISO = []
for line in lines:
    data = line.split(',')
    validISO.append(data[2])

#Load licenses for later checks
with open(ws["working"] + "/actions/dta/gbLicenses.csv") as lCsv:
    lines = lCsv.readlines()

validLicense = []
validOpenLicense = []
validAuthLicense = []
validHumLicense = []
for line in lines:
    data = line.split(',')
    validLicense.append(data[0].lower().strip())
    if(str(data[2]).strip() == "Yes"):
        validOpenLicense.append(data[0].lower().strip())
    if(str(data[3]).strip() == "Yes"):
        validAuthLicense.append(data[0].lower().strip())
    if(str(data[4]).strip() == "Yes"):
        validHumLicense.append(data[0].lower().strip())

if(len(ws["zips"]) > 0):
    gbHelpers.logWrite(checkType, "Modified zip files found.  Checking meta.txt validity.")
    gbHelpers.logWrite(checkType, "")
    zipTotal = zipTotal + 1
    for z in ws["zips"]:
        gbHelpers.checkRetrieveLFSFiles(z, ws['working'])
        req = {}
        opt = {}
        req["year"] = 0
        req["iso"] = 0
        req["bType"] = 0
        req["source"] = 0
        req["releaseType"] = 0
        req["releaseTypeName"] = ""
        req["license"] = 0
        req["licenseName"] = ""
        req["licenseSource"] = 0
        req["dataSource"] = 0
        req["releaseTypeFolder"] = 0

        opt["canonical"] = 0
        opt["licenseImage"] = 0
        opt["licenseNotes"] = 0
        opt["otherNotes"] = 0


        checkFail = 0

        gbHelpers.logWrite(checkType, "Metadata Check (" + str(zipTotal) + " of " + str(len(ws["zips"])) + "): " + z)
        bZip = zipfile.ZipFile(ws["working"] + "/" + z)
        if("meta.txt" in bZip.namelist()):
            gbHelpers.logWrite(checkType, "")
            gbHelpers.logWrite(checkType, "============================")
            gbHelpers.logWrite(checkType, "Metadata file exists in " + z)

            with zipfile.ZipFile(ws["working"] + "/" + z) as zF:
                meta = zF.read('meta.txt')
            
            for m in meta.splitlines():
                gbHelpers.logWrite(checkType, "")
                e = m.decode("latin1").split(":")
                if(len(e) > 2):
                    e[1] = e[1] + e[2]
                key = e[0].strip()
                val = e[1].strip()
                
                gbHelpers.logWrite(checkType, "Detected Key / Value: " + key + " / " + val)
                if(("Year" in key) or "year" in key):
                    year = int(float(val))
                    if( (year > 1990) and (year <= datetime.datetime.now().year)):
                        gbHelpers.logWrite(checkType, "Valid year " + str(year) + " detected.")
                        req["year"] = 1
                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: The year in the meta.txt file is invalid: " + str(year))
                        gbHelpers.logWrite(checkType, "We expect a value between 1990 and " + str(datetime.datetime.now().year))
                        checkFail = 1
                
                if("boundary type" in key.lower() and "name" not in key.lower()):
                    #May add other valid types in the future, but for now ADMs only.
                    validTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
                    if(val.upper().replace(" ","") in validTypes):
                        gbHelpers.logWrite(checkType, "Valid Boundary Type detected: " + val +".")
                        req["bType"] = 1
                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: The boundary type in the meta.txt file is invalid: " + val)
                        gbHelpers.logWrite(checkType, "We expect one of: " + str(validTypes))
                        checkFail = 1
                
                if("iso" in key.lower()):
                    if(len(val) != 3):
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: ISO is invalid - we expect a 3-character ISO code following ISO-3166-1 (Alpha 3).")
                        checkFail = 1
                    if(val not in validISO):
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: ISO is not on our list of valid ISO-3 codes.  See /actions/dta/iso_3166_1_alpha_3.csv for all valid codes this script checks against.")
                        checkFail = 1
                    else:
                        gbHelpers.logWrite(checkType, "Valid ISO detected: " + val)
                        req["iso"] = 1
                
                if("canonical" in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            gbHelpers.logWrite(checkType, "Canonical name detected: " + val)
                            opt["canonical"] = 1
                    else:
                        gbHelpers.logWrite(checkType, "WARN: No canonical name detected.  This field is optional.")
                    
                if("source" in key.lower() and "license" not in key.lower() and "data" not in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            gbHelpers.logWrite(checkType, "Source detected: " + val)
                            req["source"] = 1

                if("release type" in key.lower()):
                    if (val.lower() not in ["gbopen", "gbauthoritative", "gbhumanitarian"]):
                        gbHelpers.logWrite(checkType, "Invalid release type detected: " + val)
                        gbHelpers.logWrite(checkType, "We expect one of three values: gbOpen, gbAuthoritative, and gbHumanitarian")
                        checkFail = 1
                    else:
                        if(val.lower() not in z.lower()):
                            req["releaseTypeName"] = val.lower().strip()
                            req["releaseType"] = 1
                            req["releaseTypeFolder"] = 0
                            gbHelpers.logWrite(checkType, "CRITICAL ERROR: The zip file is in the incorrect subdirectory - according to meta.txt you are submitting a " + val + " boundary, but have the zip file in the folder " + z + ".")
                            checkFail = 1
                        else:
                            req["releaseType"] = 1
                            req["releaseTypeName"] = val.lower().strip()
                            req["releaseTypeFolder"] = 1

                if("license" == key.lower()):
                    if(('"' + val.lower().strip() + '"') not in validLicense):
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: Invalid license detected: " + val)
                        gbHelpers.logWrite(checkType, "We expect one of the licenses in /actions/dta/gbLicenses.csv.  If you believe your license should be included, please open a ticket.")
                        checkFail = 1
                    else:
                        req["license"] = 1
                        req["licenseName"] = val.lower().strip()
                        gbHelpers.logWrite(checkType, "Valid license type detected: " + val)
                        

                if("license notes" in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            gbHelpers.logWrite(checkType, "License notes detected: " + val)
                            opt["licenseNotes"] = 1
                    else:
                        gbHelpers.logWrite(checkType, "WARN: No license notes detected.  This field is optional.")

                if("license source" in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            gbHelpers.logWrite(checkType, "License source detected: " + val)
                            req["licenseSource"] = 1
                            #Check for a png image of the license source.
                            #Any png or jpg with the name "license" is accepted.
                            licPic = 0
                            try:
                                with zipfile.ZipFile(ws["working"] + "/" + z) as zFb:
                                    licPic = zFb.read('license.png')
                            except:
                                pass

                            try:
                                with zipfile.ZipFile(ws["working"] + "/" + z) as zFb:
                                    licPic = zFb.read('license.jpg')
                            except:
                                pass

                            if(licPic != 0):
                                gbHelpers.logWrite(checkType, "License image found.")
                                opt["licenseImage"] = 1
                            else:
                                gbHelpers.logWrite(checkType, "WARN: No license image found.  This is not required.  We check for license.png and license.jpg.")
                        
                        else:
                            gbHelpers.logWrite(checkType, "CRITICAL ERROR: No license source detected.")
                            checkFail = 1


                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: No license source detected.")
                        checkFail = 1

                if("link to source data" in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            req["dataSource"] = 1
                            gbHelpers.logWrite(checkType, "Data Source Found: " + val)
                                            
                        else:
                            gbHelpers.logWrite(checkType, "CRITICAL ERROR: No license source detected.")
                            checkFail = 1


                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: No license source detected.")
                        checkFail = 1

                if("other notes" in key.lower()):
                    if(len(val.replace(" ","")) > 0):
                        if(val.lower() not in ["na", "nan", "null"]):
                            gbHelpers.logWrite(checkType, "Other notes detected: " + val)
                            opt["otherNotes"] = 1
                    else:
                        gbHelpers.logWrite(checkType, "WARN: No other notes detected.  This field is optional.")


            if((req["license"] == 1) and (req["releaseType"] == 1)):
                gbHelpers.logWrite(checkType, "")
                gbHelpers.logWrite(checkType, "Both a license and release type are defined.  Checking for compatability.")
                if(req["releaseTypeName"] == "gbopen"):
                    if(('"' + req["licenseName"] + '"') in validOpenLicense):
                        gbHelpers.logWrite(checkType, "License type is a valid license for the gbOpen product.")
                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: The license you have specified is not valid for the gbOpen product.")
                        checkFail = 1
                
                if(req["releaseTypeName"] == "gbauthoritative"):
                    if(('"' + req["licenseName"] + '"') in validAuthLicense): 
                        gbHelpers.logWrite(checkType, "License type is a valid license for the gbAuthoritative product.")
                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: The license you have specified is not valid for the gbAuthoritative product.")
                        checkFail = 1

                if(req["releaseTypeName"] == "gbhumanitarian"):
                    if(('"' + req["licenseName"] + '"') in validHumLicense): 
                        gbHelpers.logWrite(checkType, "License type is a valid license for the gbHumanitarian product.")
                    else:
                        gbHelpers.logWrite(checkType, "CRITICAL ERROR: The license you have specified is not valid for the gbHumanitarian product.")
                        checkFail = 1





            if(req["source"] == 0):
                gbHelpers.logWrite(checkType, "CRITICAL ERROR: No data source was provided in the metadata.")
                checkFail = 1

      

            gbHelpers.logWrite(checkType, "")
            gbHelpers.logWrite(checkType, "Metadata checks complete for " + z)
            gbHelpers.logWrite(checkType, "")
            gbHelpers.logWrite(checkType, "----------------------------")
            gbHelpers.logWrite(checkType, "      OPTIONAL TESTS        ")
            gbHelpers.logWrite(checkType, "----------------------------")
            for i in opt:
                if(opt[i] == 1 or len(str(opt[i]))>1):
                    gbHelpers.logWrite(checkType, '%-20s%-12s' % (i, "PASSED"))
                else:
                    gbHelpers.logWrite(checkType, '%-20s%-12s' % (i, "FAILED"))
            gbHelpers.logWrite(checkType, "")
            gbHelpers.logWrite(checkType, "----------------------------")
            gbHelpers.logWrite(checkType, "      REQUIRED TESTS        ")
            gbHelpers.logWrite(checkType, "----------------------------")
            for i in req:
                if(req[i] == 1 or len(str(req[i]))>1):
                    gbHelpers.logWrite(checkType, '%-20s%-12s' % (i, "PASSED"))
                else:
                    gbHelpers.logWrite(checkType, '%-20s%-12s' % (i, "FAILED"))
            gbHelpers.logWrite(checkType, "==========================")
            
            

        else:
            gbHelpers.logWrite(checkType, "CRITICAL ERROR: Metadata file does not exist in " + z)
            gbHelpers.gbEnvVars("RESULT", "CRITICAL ERROR: Metadata file does not exist in " + z, "w")
            checkFail = 1
        
        
        
        if(checkFail == 1):
            zipFailures = zipFailures + 1
            anyFail = 1
            
        else:
            zipSuccess = zipSuccess + 1
            gbHelpers.logWrite(checkType, "Metadata checks passed for " + z)

    gbHelpers.logWrite(checkType, "")
    gbHelpers.logWrite(checkType, "====================")
    gbHelpers.logWrite(checkType, "All metadata checks complete.")
    gbHelpers.logWrite(checkType, "Successes: " + str(zipSuccess))
    gbHelpers.logWrite(checkType, "Failures: " + str(zipFailures))
    

    if(zipFailures > 0):
        gbHelpers.logWrite(checkType, "CRITICAL ERROR: At least one Metadata check failed; check the log to see what's wrong.")  
        gbHelpers.gbEnvVars("RESULT", "It looks like your metadata has one or more errors - take a look at the logs to see what you need to fix.", "w")      
    else:
        gbHelpers.gbEnvVars("RESULT", "PASSED", "w")

else:
    gbHelpers.logWrite(checkType, "CRITICAL ERROR: No modified zip files found.")
    gbHelpers.gbEnvVars("RESULT", "You didn't submit a zip file.", "w")
    