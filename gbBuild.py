import os
import sys
import gbHelpers
import gbDataCheck
import zipfile
import gbMetaCheck
import csv
import json
from distutils.dir_util import copy_tree

buildType = str(sys.argv[1])
buildVer = str(sys.argv[2])
fullBuild = str(sys.argv[3])
ws = gbHelpers.initiateWorkspace(buildType, build = True)
csvR = []

for (path, dirname, filenames) in os.walk(ws["working"] + "/sourceData/" + buildType + "/"):
    for filename in filenames:
        row = {}
        row["META_requiredChecksPassing"] = 0
        row["GEOM_requiredChecksPassing"] = 0
        ws["zipSuccess"] = 0
        ws['zips'] = []
        ws['zips'].append("/sourceData/" + buildType + "/" + filename)
        gbHelpers.checkRetrieveLFSFiles(ws['zips'][0], ws['working'])
        try:
            with zipfile.ZipFile(ws["working"] + "/" + ws['zips'][0]) as zF:
                meta = zF.read('meta.txt')
        except:
            if(buildVer == "dev"):
                row["status"] = "FAILING; no meta.txt"
            else:
                print("No meta.txt in at least one file.  To make a release build, all checks must pass.  Try running a dev build first. Exiting.")
                sys.exit(1)
        
        #Check that the meta.txt is passing all checks.
        metaChecks = gbMetaCheck.metaCheck(ws)

        if(metaChecks[2] != 1 and buildVer != "dev"):
            print("At least one metadata check is failing, so you cannot make a release build.  Try a dev build first. Here is what we know :" + str(metaChecks))

        if(buildVer == "dev"):
            row["META_requiredChecksPassing"] = bool(metaChecks[2])
            row["META_canonicalNameInMeta"] = bool(metaChecks[0]['canonical'])
            row["META_licenseImageInZip"] = bool(metaChecks[0]['licenseImage'])
            row["META_yearValid"] = bool(metaChecks[1]['year'])
            row["META_isoValid"] = bool(metaChecks[1]["iso"])
            row["META_boundaryTypeValid"] = bool(metaChecks[1]["bType"])
            row["META_sourceExists"] = bool(metaChecks[1]["source"])
            row["META_releaseTypeValid"] = bool(metaChecks[1]["releaseType"])
            row["META_releaseTypeCorrectFolder"] = bool(metaChecks[1]["releaseTypeFolder"])
            row["META_licenseValid"] = bool(metaChecks[1]["license"])
            row["META_licenseSourceExists"] = bool(metaChecks[1]["licenseSource"])
            row["META_dataSourceExists"] = bool(metaChecks[1]["dataSource"])

        #Run the automated geometry checks
        geomChecks = gbDataCheck.geometryCheck(ws)

        if(geomChecks[2] != 1 and buildVer != "dev"):
            print("At least one geometry check is failing, so you cannot make a release build.  Try a dev build first. Here is what we know :" + str(geomChecks))
            sys.exit()

        if(buildVer == "dev"):
            row["GEOM_requiredChecksPassing"] = bool(geomChecks[2])
            row["GEOM_boundaryNamesColumnExists"] = bool(geomChecks[0]["bndName"])
            row["GEOM_boundaryNamesFilledIn"] = bool(geomChecks[0]["nameCount"])
            row["GEOM_boundaryISOColumnExists"] = bool(geomChecks[0]["bndISO"])
            row["GEOM_boundaryISOsFilledIn"] = bool(geomChecks[0]["isoCount"])
            row["GEOM_Topology"] = bool(geomChecks[0]["topology"])
            row["GEOM_Projection"] = bool(geomChecks[1]["proj"])

        #Build release columns
        zipMeta = {}
            
        for m in meta.splitlines():
            e = m.decode("latin1").split(":")
            if(len(e) > 2):
                e[1] = e[1] + e[2]
            key = e[0].strip()
            val = e[1].strip()
            zipMeta[key] = val
        
        row["boundaryID"] = zipMeta['ISO-3166-1 (Alpha-3)'] + "-" + zipMeta["Boundary Type"] + "-" + buildVer
        row["boundaryISO"] = zipMeta['ISO-3166-1 (Alpha-3)']
        row["boundaryYear"] = zipMeta["Boundary Representative of Year"]
        row["boundaryType"] = zipMeta["Boundary Type"]
        row["boundarySource_1"] = zipMeta["Source 1"]
        row["boundarySource_2"] = zipMeta["Source 2"]
        row["boundaryCanonical"] = zipMeta["Canonical Boundary Type Name"]
        row["boundaryLicense"] = zipMeta["License"]
        row["licenseDetail"] = zipMeta["License Notes"]
        row["licenseSource"] = zipMeta["License Source"]
        row["boundarySourceURL"] = zipMeta["Link to Source Data"]
        row["downloadURL"] = "https://github.com/wmgeolab/gbRelease/raw/master/releaseData/" + str(buildType) + "/" + str(filename)
        
        #Build high level structure
        if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/"):
            os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/")

        if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/"):
            os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/")

        if(fullBuild == "True"):
            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/")

            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/")

        
        if(fullBuild == "True"):
            #Build JSON and TXT meta
            basePath = os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/"
            #with open("geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.json")

            #Remember the attribute table standardization
            #Shapely buffer 0



            #Need to build the actual zip files and move things around.
            #Make a temp copy in /tmp, then we'll copy to the gbRelease repository at the end.
            zipPath = row["boundaryISO"] + "-" + row["boundaryType"] + "-geoBoundaries-" + buildType + "-all.zip"
            
            
            #Note we need to simplify the boundaries and include them in the zip as well.

            #Make map preview images
            #dta.boundary.plot()
            #plt.savefig(os.path.expanduser("~") + "/tmp/preview.png")

        csvR.append(row)

#Build the CSV - if fullbuild = False, this is all that gets pushed.
keys = csvR[0].keys()
with open(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(buildType) + "_metaData.csv", "w") as f:
    writer = csv.DictWriter(f, keys)
    writer.writeheader()
    writer.writerows(csvR)

#Copy the tmp directory over to the main repository

if(ws["working"] != "/home/dan/git/gbRelease"):
    try:
        os.remove(os.path.expanduser("~")+"/tmp/RESULT.TXT")
    except:
        print("Cleanup skipped.  Proceeding with Upload.")
    os.system("ls " + ws["working"])
    copy_tree(os.path.expanduser("~") + "/tmp/", ws["working"])
    os.system("ls " + ws["working"])
