import datetime
import zipfile

import gbHelpers


def metaCheck(ws):
    #Load ISOs for later checks
    with open("dta/iso_3166_1_alpha_3.csv") as isoCsv:
        lines = isoCsv.readlines()

    validISO = []
    for line in lines:
        data = line.split(',')
        validISO.append(data[2])

    print(validISO)

    #Load licenses for later checks
    with open("dta/gbLicenses.csv") as lCsv:
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
        gbHelpers.logWrite(ws, "Modified zip files found.  Checking meta.txt validity.")
        gbHelpers.logWrite(ws, "")
        ws["zipTotal"] = ws["zipTotal"] + 1
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

            gbHelpers.logWrite(ws, "Metadata Check: " + z)
            try:
                bZip = zipfile.ZipFile(ws["working"] + "/" + z)
            except:
                print("A zipfile didn't open.  " + str(z))
                return [opt, req, 0]
            if("meta.txt" in bZip.namelist()):
                gbHelpers.logWrite(ws, "")
                gbHelpers.logWrite(ws, "============================")
                gbHelpers.logWrite(ws, "Metadata file exists in " + z)

                with zipfile.ZipFile(ws["working"] + "/" + z) as zF:
                    meta = zF.read('meta.txt')

                for m in meta.splitlines():
                    try:
                        gbHelpers.logWrite(ws, "")
                        e = m.decode("utf-8").split(":")
                        if(len(e) > 2):
                            e[1] = e[1] + e[2]
                        key = e[0].strip()
                        val = e[1].strip()
                    except:
                        checkFail = 1
                        gbHelpers.logWrite(ws, "WARN: At least one line of the meta.txt failed to be read correctly: " + str(m))
                        key = "readError"
                        val = "readError"

                    gbHelpers.logWrite(ws, "Detected Key / Value: " + key + " / " + val)
                    if(("Year" in key) or "year" in key):
                        try:
                            if "to" in val:
                                date1, date2 = val.split(" to ")
                                date1 = datetime.datetime.strptime(date1, "%d-%m-%Y")
                                date2 = datetime.datetime.strptime(date2, "%d-%m-%Y")
                                gbHelpers.logWrite(ws, "Valid date range " + str(val) + " detected.")
                                req["year"] = 1
                            else:
                                year = int(float(val))
                                if( (year > 1950) and (year <= datetime.datetime.now().year)):
                                    gbHelpers.logWrite(ws, "Valid year " + str(year) + " detected.")
                                    req["year"] = 1
                                else:
                                    gbHelpers.logWrite(ws, "CRITICAL ERROR: The year in the meta.txt file is invalid: " + str(year))
                                    gbHelpers.logWrite(ws, "We expect a value between 1950 and " + str(datetime.datetime.now().year))
                                    checkFail = 1
                        except:
                                gbHelpers.logWrite(ws, "CRITICAL ERROR: The year in the meta.txt file is invalid.")
                                checkFail = 1

                    if("boundary type" in key.lower() and "name" not in key.lower()):
                        #May add other valid types in the future, but for now ADMs only.
                        validTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
                        if(val.upper().replace(" ","") in validTypes):
                            gbHelpers.logWrite(ws, "Valid Boundary Type detected: " + val +".")
                            req["bType"] = 1
                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: The boundary type in the meta.txt file is invalid: " + val)
                            gbHelpers.logWrite(ws, "We expect one of: " + str(validTypes))
                            checkFail = 1

                    if("iso" in key.lower().strip()):
                        if(len(val) != 3):
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: ISO is invalid - we expect a 3-character ISO code following ISO-3166-1 (Alpha 3).")
                            checkFail = 1
                        if(val not in validISO):
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: ISO is not on our list of valid ISO-3 codes.  See https://github.com/wmgeolab/geoBoundaryBot/blob/main/dta/iso_3166_1_alpha_3.csv for all valid codes this script checks against.")
                            checkFail = 1
                        else:
                            gbHelpers.logWrite(ws, "Valid ISO detected: " + val)
                            req["iso"] = 1

                    if("canonical" in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                gbHelpers.logWrite(ws, "Canonical name detected: " + val)
                                opt["canonical"] = 1
                        else:
                            gbHelpers.logWrite(ws, "WARN: No canonical name detected.  This field is optional.")

                    if("source" in key.lower() and "license" not in key.lower() and "data" not in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                gbHelpers.logWrite(ws, "Source detected: " + val)
                                req["source"] = 1

                    if("release type" in key.lower()):
                        if (val.lower() not in ["gbopen", "gbauthoritative", "gbhumanitarian"]):
                            gbHelpers.logWrite(ws, "Invalid release type detected: " + val)
                            gbHelpers.logWrite(ws, "We expect one of three values: gbOpen, gbAuthoritative, and gbHumanitarian")
                            checkFail = 1
                        else:
                            if(val.lower() not in z.lower()):
                                req["releaseTypeName"] = val.lower().strip()
                                req["releaseType"] = 1
                                req["releaseTypeFolder"] = 0
                                gbHelpers.logWrite(ws, "CRITICAL ERROR: The zip file is in the incorrect subdirectory - according to meta.txt you are submitting a " + val + " boundary, but have the zip file in the folder " + z + ".")
                                checkFail = 1
                            else:
                                req["releaseType"] = 1
                                req["releaseTypeName"] = val.lower().strip()
                                req["releaseTypeFolder"] = 1

                    if("license" == key.lower()):
                        if(('"' + val.lower().strip() + '"') not in validLicense):
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: Invalid license detected: " + val)
                            gbHelpers.logWrite(ws, "We expect one of the licenses in https://github.com/wmgeolab/geoBoundaryBot/blob/main/dta/gbLicenses.csv.  It must exactly match one of these; we do no fuzzy matching to ensure accuracy. If you believe your license should be included, please open a ticket.")
                            checkFail = 1
                        else:
                            req["license"] = 1
                            req["licenseName"] = val.lower().strip()
                            gbHelpers.logWrite(ws, "Valid license type detected: " + val)


                    if("license notes" in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                gbHelpers.logWrite(ws, "License notes detected: " + val)
                                opt["licenseNotes"] = 1
                        else:
                            gbHelpers.logWrite(ws, "WARN: No license notes detected.  This field is optional.")

                    if("license source" in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                gbHelpers.logWrite(ws, "License source detected: " + val)
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
                                    gbHelpers.logWrite(ws, "License image found.")
                                    opt["licenseImage"] = 1
                                else:
                                    gbHelpers.logWrite(ws, "WARN: No license image found.  This is not required.  We check for license.png and license.jpg.")

                            else:
                                gbHelpers.logWrite(ws, "CRITICAL ERROR: No license source detected.")
                                checkFail = 1


                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: No license source detected.")
                            checkFail = 1

                    if("link to source data" in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                req["dataSource"] = 1
                                gbHelpers.logWrite(ws, "Data Source Found: " + val)

                            else:
                                gbHelpers.logWrite(ws, "CRITICAL ERROR: No license source detected.")
                                checkFail = 1


                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: No license source detected.")
                            checkFail = 1

                    if("other notes" in key.lower()):
                        if(len(val.replace(" ","")) > 0):
                            if(val.lower() not in ["na", "nan", "null"]):
                                gbHelpers.logWrite(ws, "Other notes detected: " + val)
                                opt["otherNotes"] = 1
                        else:
                            gbHelpers.logWrite(ws, "WARN: No other notes detected.  This field is optional.")


                if((req["license"] == 1) and (req["releaseType"] == 1)):
                    gbHelpers.logWrite(ws, "")
                    gbHelpers.logWrite(ws, "Both a license and release type are defined.  Checking for compatability.")
                    if(req["releaseTypeName"] == "gbopen"):
                        if(('"' + req["licenseName"] + '"') in validOpenLicense):
                            gbHelpers.logWrite(ws, "License type is valid license for the gbOpen product.")
                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: The license you have specified is not valid for the gbOpen product.")
                            checkFail = 1

                    if(req["releaseTypeName"] == "gbauthoritative"):
                        if(('"' + req["licenseName"] + '"') in validAuthLicense):
                            gbHelpers.logWrite(ws, "License type is a valid license for the gbAuthoritative product.")
                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: The license you have specified is not valid for the gbAuthoritative product.")
                            checkFail = 1

                    if(req["releaseTypeName"] == "gbhumanitarian"):
                        if(('"' + req["licenseName"] + '"') in validHumLicense):
                            gbHelpers.logWrite(ws, "License type is a valid license for the gbHumanitarian product.")
                        else:
                            gbHelpers.logWrite(ws, "CRITICAL ERROR: The license you have specified is not valid for the gbHumanitarian product.")
                            checkFail = 1





                if(req["source"] == 0):
                    gbHelpers.logWrite(ws, "CRITICAL ERROR: No data source was provided in the metadata.")
                    checkFail = 1



                gbHelpers.logWrite(ws, "")
                gbHelpers.logWrite(ws, "Metadata checks complete for :" + z)
                gbHelpers.logWrite(ws, "")
                gbHelpers.logWrite(ws, "----------------------------")
                gbHelpers.logWrite(ws, "      OPTIONAL TESTS        ")
                gbHelpers.logWrite(ws, "----------------------------")
                for i in opt:
                    if(opt[i] == 1 or len(str(opt[i]))>1):
                        gbHelpers.logWrite(ws, '%-20s%-12s' % (i, "PASSED"))
                    else:
                        gbHelpers.logWrite(ws, '%-20s%-12s' % (i, "FAILED"))
                gbHelpers.logWrite(ws, "")
                gbHelpers.logWrite(ws, "----------------------------")
                gbHelpers.logWrite(ws, "      REQUIRED TESTS        ")
                gbHelpers.logWrite(ws, "----------------------------")
                for i in req:
                    if(req[i] == 1 or len(str(req[i]))>1):
                        gbHelpers.logWrite(ws, '%-20s%-12s' % (i, "PASSED"))
                    else:
                        gbHelpers.logWrite(ws, '%-20s%-12s' % (i, "FAILED"))
                        checkFail = 1
                gbHelpers.logWrite(ws, "==========================")

            else:
                gbHelpers.logWrite(ws, "CRITICAL ERROR: Metadata file does not exist in " + z)
                gbHelpers.gbEnvVars("RESULT", "CRITICAL ERROR: Metadata file does not exist in " + z, "w")
                checkFail = 1



            if(checkFail == 1):
                ws["zipFailures"] = ws["zipFailures"] + 1

            else:
                ws["zipSuccess"] = ws["zipSuccess"] + 1
                gbHelpers.logWrite(ws, "Metadata checks passed for " + z)

        gbHelpers.logWrite(ws, "")
        gbHelpers.logWrite(ws, "====================")
        gbHelpers.logWrite(ws, "All metadata checks complete.")
        gbHelpers.logWrite(ws, "Successes: " + str(ws["zipSuccess"]))
        gbHelpers.logWrite(ws, "Failures: " + str(ws["zipFailures"]))

        if(ws["zipFailures"] > 0):
            gbHelpers.logWrite(ws, "CRITICAL ERROR: At least one Metadata check failed; check the log to see what's wrong.")
            gbHelpers.gbEnvVars("RESULT", "It looks like your metadata has one or more errors - take a look at the logs to see what you need to fix.", "w")
        else:
            gbHelpers.gbEnvVars("RESULT", "PASSED", "w")

        #Return of the last element for overall build
        return [opt, req, ws["zipSuccess"]]

    else:
        gbHelpers.logWrite(ws, "CRITICAL ERROR: No modified zip files found.")
        gbHelpers.gbEnvVars("RESULT", "You didn't submit a zip file.", "w")

if __name__ == "__main__":
    ws = gbHelpers.initiateWorkspace("metaChecks")
    metaCheck(ws)

