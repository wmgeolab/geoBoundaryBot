import zipfile

import gbHelpers

checkType = "fileChecks"
ws = gbHelpers.initiateWorkspace(checkType)

zipFailures = 0
zipSuccess = 0
zipTotal = 0
anyFail = 0

if len(ws["zips"]) > 0:
    gbHelpers.logWrite(ws, "Modified zip files found.  Downloading and checking validity.")
    gbHelpers.logWrite(ws, "")
    zipTotal = zipTotal + 1
    for z in ws["zips"]:
        checkFail = 0

        gbHelpers.checkRetrieveLFSFiles(z, ws["working"])
        gbHelpers.logWrite(ws, "File Check (" + str(zipTotal) + " of " + str(len(ws["zips"])) + "): " + z)
        bZip = zipfile.ZipFile(ws["working"] + "/" + z)

        if "meta.txt" in bZip.namelist():
            gbHelpers.logWrite(ws, "Metadata file exists in " + z)
        else:
            gbHelpers.logWrite(ws, "CRITICAL ERROR: Metadata file does not exist in " + z)
            gbHelpers.gbEnvVars(
                "RESULT",
                "You submitted at least one file that is missing a meta.txt, which is required.  Please make sure your meta.txt is in the root of your submitted zip file - I have not yet been programmed to look in folders for your meta.txt.",
                "w",
            )
            checkFail = 1

        geojson = list(filter(lambda x: x[-8:] == ".geojson", bZip.namelist()))
        shp = list(filter(lambda x: x[-4:] == ".shp", bZip.namelist()))
        geojson = [x for x in geojson if not x.__contains__("MACOS")]
        shp = [x for x in shp if not x.__contains__("MACOS")]
        allShps = geojson + shp
        if len(allShps) == 1:
            if len(shp) == 1:
                gbHelpers.logWrite(ws, "Shapefile (*.shp) found. Checking if all required files are present.")
                if len(list(filter(lambda x: x[-4:] == ".shx", bZip.namelist()))) < 1:
                    gbHelpers.logWrite(
                        ws, "CRITICAL ERROR: A valid *.shp requires a *.shx (index) file. None was found in " + z
                    )
                    gbHelpers.gbEnvVars(
                        "RESULT",
                        "You submitted a *.shp file without a *.shx file, which is required (*.shx, *.dbf, and *.prj are all required)",
                        "w",
                    )
                    checkFail = 1
                else:
                    gbHelpers.logWrite(ws, ".shx found.")
                if len(list(filter(lambda x: x[-4:] == ".dbf", bZip.namelist()))) < 1:
                    gbHelpers.logWrite(
                        ws, "CRITICAL ERROR: A valid *.shp requires a *.dbf (index) file. None was found in " + z
                    )
                    gbHelpers.gbEnvVars(
                        "RESULT",
                        "You submitted a *.shp file without a *.dbf file, which is required (*.shx, *.dbf, and *.prj are all required).",
                        "w",
                    )
                    checkFail = 1
                else:
                    gbHelpers.logWrite(ws, ".dbf found.")
                if len(list(filter(lambda x: x[-4:] == ".prj", bZip.namelist()))) < 1:
                    gbHelpers.logWrite(
                        ws, "CRITICAL ERROR: A valid *.shp requires a *.prj (index) file. None was found in " + z
                    )
                    gbHelpers.gbEnvVars(
                        "RESULT",
                        "You submitted a *.shp file without a *.prj file, which is required (*.shx, *.dbf, and *.prj are all required).",
                        "w",
                    )
                    checkFail = 1
                else:
                    gbHelpers.logWrite(ws, ".prj found.")

            if len(geojson) == 1:
                gbHelpers.logWrite(ws, "geoJSON found.")

        if len(allShps) == 0:
            gbHelpers.logWrite(ws, "CRITICAL ERROR: No *.shp or *.geojson found for " + z)
            gbHelpers.gbEnvVars(
                "RESULT",
                "I couldn't find a *.shp or *.geojson in the zip file you provided.  Make sure all of your files are in the root - i.e., your file shouldn't have a folder inside it.",
                "w",
            )
            checkFail = 1
        if len(allShps) > 1:
            gbHelpers.logWrite(ws, "CRITICAL ERROR: More than one geometry file (*.shp, *.geojson) was found for " + z)
            gbHelpers.gbEnvVars(
                "RESULT",
                "At least one file you submitted had more than one geometry file in it (i.e., multiple *.shp or *.geojson).",
                "w",
            )
            checkFail = 1

        if checkFail == 1:
            zipFailures = zipFailures + 1
            gbHelpers.logWrite(
                ws, "CRITICAL ERROR: Zipfile validity checks failed for " + z + ".  Check the log to see what is wrong."
            )
        else:
            zipSuccess = zipSuccess + 1
            gbHelpers.logWrite(ws, "Zipfile validity checks passed for " + z)

    gbHelpers.logWrite(ws, "")
    gbHelpers.logWrite(ws, "====================")
    gbHelpers.logWrite(ws, "All zip validity checks complete.")
    gbHelpers.logWrite(ws, "Successes: " + str(zipSuccess))
    gbHelpers.logWrite(ws, "Failures: " + str(zipFailures))
    if zipFailures > 0:
        gbHelpers.logWrite(ws, "CRITICAL ERROR: At least one file check failed; check the log to see what's wrong.")
    else:
        print("Set")
        gbHelpers.gbEnvVars("RESULT", "PASSED", "w")

else:
    gbHelpers.logWrite(ws, "No modified zip files found.")
    gbHelpers.gbEnvVars("RESULT", "You didn't submit a zip file.", "w")
