import os
import sys
import zipfile
import subprocess

#For testing
try:
    working = os.environ['GITHUB_WORKSPACE']
    working = os.environ['GITHUB_WORKSPACE']
    changedFiles = os.environ['changes'].strip('][').split(',')
    logDir = "~/tmp"
except:
    working = "/home/dan/git/gbRelease"
    logDir = working + "/tmp/sha"
print("Python WD: " + working)  

#For testing
try:
    changedFiles = os.environ['changes'].strip('][').split(',')
except:
    changedFiles = ['.github/workflows/gbPush.yml', 'sourceData/gbOpen/ARE_ADM1.zip', 'sourceData/gbOpen/QAT_ADM0.zip']

def logWrite(line):
    print(line)
    with open(logDir + "/" + "metaCheckLog.txt", "a") as f:
        f.write(line + "\n")


logWrite("Python changedFiles: " + str(changedFiles))


#Check that zip files exist in the request
zips = list(filter(lambda x: x[-4:] == '.zip', changedFiles))

zipFailures = 0
zipSuccess = 0
zipTotal = 0

if(len(zips) > 0):
    logWrite("Modified zip files found.  Downloading and checking validity.")
    logWrite("")
    zipTotal = zipTotal + 1
    for z in zips:
        checkFail = 0
        logWrite("")
        logWrite("Downloading: " + z)
        try:
            dl = os.system('git lfs pull --include=\"' + z +'\"')
        except:
            logWrite("No file on LFS to retrieve.  Continuing.")
        logWrite("File Check (" + str(zipTotal) + " of " + str(len(zips)) + "): " + z)
        bZip = zipfile.ZipFile(working + "/" + z)
        if("meta.txt" in bZip.namelist()):
            logWrite("Metadata file exists in " + z)
        else:
            logWrite("CRITICAL ERROR: Metadata file does not exist in " + z)
            checkFail = 1
        
        geojson = list(filter(lambda x: x[-4:] == '.geojson', bZip.namelist()))
        shp = list(filter(lambda x: x[-4:] == '.shp', bZip.namelist()))
        allShps = geojson + shp 
        if(len(allShps) == 1):
            if(len(shp) == 1):
                logWrite("Shapefile (*.shp) found. Checking if all required files are present.")
                if(len(list(filter(lambda x: x[-4:] == '.shx', bZip.namelist()))) != 1):
                    logWrite("CRITICAL ERROR: A valid *.shp requires a *.shx (index) file. None was found in " + z)
                    checkFail = 1
                else:
                    logWrite(".shx found.")
                if(len(list(filter(lambda x: x[-4:] == '.dbf', bZip.namelist()))) != 1):
                    logWrite("CRITICAL ERROR: A valid *.shp requires a *.dbf (index) file. None was found in " + z)
                    checkFail = 1
                else:
                    logWrite(".dbf found.")
                if(len(list(filter(lambda x: x[-4:] == '.prj', bZip.namelist()))) != 1):
                    logWrite("CRITICAL ERROR: A valid *.shp requires a *.prj (index) file. None was found in " + z)
                    checkFail = 1
                else:
                    logWrite(".prj found.")

            if(len(geojson) == 1):
                logWrite("geoJSON found.")

        if(len(allShps) == 0):
            logWrite("CRITICAL ERROR: No *.shp or *.geojson found for " + z)
            checkFail = 1
        if(len(allShps) > 1):
            logWrite("CRITICAL ERROR: More than one geometry file (*.shp, *.geojson) was found for " + z)
            checkFail = 1
        
        if(checkFail == 1):
            zipFailures = zipFailures + 1
            logWrite("CRITICAL ERROR: Zipfile validity checks failed for " + z + ".  Check the log to see what is wrong.")
        else:
            zipSuccess = zipSuccess + 1
            logWrite("Zipfile validity checks passed for " + z)

    logWrite("")
    logWrite("====================")
    logWrite("All zip validity checks complete.")
    logWrite("Successes: " + str(zipSuccess))
    logWrite("Failures: " + str(zipFailures))
    if(zipFailures > 0):
        sys.exit("CRITICAL ERROR: At least one Metadata check failed; check the log to see what's wrong.")

else:
    logWrite("No modified zip files found.")
    sys.exit("Error: No zip files found!")
