import os
import sys
import gbHelpers
import gbDataCheck
import zipfile
import gbMetaCheck
import csv
import json
from distutils.dir_util import copy_tree
import time
import geopandas
import shutil
import hashlib
import matplotlib.pyplot as plt
from shapely.geometry.polygon import Polygon
from shapely.geometry.multipolygon import MultiPolygon
from datetime import datetime
import requests

buildType = str(sys.argv[1])
buildVer = str(sys.argv[2])
cQuery = str(sys.argv[3])
typeQuery = str(sys.argv[4])
APIkey = str(sys.argv[5])
ws = gbHelpers.initiateWorkspace(buildType, build = True)
print(ws)
csvR = []
bCnt = 0
issueCreationCount = 0
issueCommentCount = 0


for (path, dirname, filenames) in os.walk(ws["working"] + "/sourceData/" + buildType + "/"):
    selFiles = []
    for i in cQuery.split(","):
        selFiles.append([x for x in filenames if x.startswith(i + "_" + typeQuery)])
    filesToProcess = [item for sublist in selFiles for item in sublist]
    print(filesToProcess)
    for filename in filesToProcess:
        bCnt = bCnt + 1
        print("Processing " + str(filename) + " (boundary " + str(bCnt) + ")")
        row = {}
        row["status"] = ""
        row["META_requiredChecksPassing"] = 0
        row["GEOM_requiredChecksPassing"] = 0
        ws["zipSuccess"] = 0

        ws['zips'] = []
        ws['zips'].append("/sourceData/" + buildType + "/" + filename)


        gbHelpers.checkRetrieveLFSFiles(ws['zips'][0], ws['working'])
        try:
            
            with zipfile.ZipFile(ws["working"] + "/" + ws['zips'][0]) as zF:
                meta = zF.read('meta.txt')
            
            m = hashlib.sha256()
            chunkSize = 8192
            with open(ws["working"] + "/" + ws['zips'][0], 'rb') as zF:
                while True:
                    chunk = zF.read(chunkSize)
                    if(len(chunk)):
                        m.update(chunk)
                    else:
                        break
                #8 digit modulo on the hash.  Won't guarantee unique,
                #but as this is per ADM/ISO, collision is very (very) unlikely.
                metaHash = int(m.hexdigest(), 16) % 10**8
                print(metaHash)

        except:
            if(buildVer == "nightly"):
                row["status"] = "FAIL"
            else:
                print("No meta.txt in at least one file.  To make a release build, all checks must pass.  Try running a nightly build first. Exiting.")
                sys.exit(1)
        
        #Check that the meta.txt is passing all checks.
        print("Processing metadata checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
        metaChecks = gbMetaCheck.metaCheck(ws)

        if(metaChecks[2] != 1 and buildVer != "nightly"):
            print("At least one metadata check is failing, so you cannot make a release build.  Try a nightly build first. Here is what we know :" + str(metaChecks))

        if(buildVer == "nightly"):
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
        print("Processing geometry checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
        print(ws)
        geomChecks = gbDataCheck.geometryCheck(ws)

        if(geomChecks[2] != 1 and buildVer != "nightly"):
            print("At least one geometry check is failing, so you cannot make a release build.  Try a nightly build first. Here is what we know :" + str(geomChecks))
            sys.exit()

        if(buildVer == "nightly"):
            row["GEOM_requiredChecksPassing"] = bool(geomChecks[2])
            row["GEOM_boundaryNamesColumnExists"] = bool(geomChecks[0]["bndName"])
            row["GEOM_boundaryNamesFilledIn"] = bool(geomChecks[0]["nameCount"])
            row["GEOM_boundaryISOColumnExists"] = bool(geomChecks[0]["bndISO"])
            row["GEOM_boundaryISOsFilledIn"] = bool(geomChecks[0]["isoCount"])
            row["GEOM_Topology"] = bool(geomChecks[0]["topology"])
            row["GEOM_Projection"] = bool(geomChecks[1]["proj"])

        #Build release columns
        zipMeta = {}
        row["boundaryID"] = "METADATA ERROR"
        row["boundaryISO"] = "METADATA ERROR"
        row["boundaryType"] = "METADATA ERROR"

        for m in meta.splitlines():
            e = m.decode("utf-8").split(":")
            if(len(e) > 2):
                e[1] = e[1] + e[2]
            key = e[0].strip()
            try:
                val = e[1].strip()
            except:
                if(buildVer == "nightly"):
                    row["status"] = "FAIL"
                else:
                    print("The meta.txt file was not parsed correctly for at least one file.  To make a release build, all checks must pass.  Try running a nightly build first. Exiting.")
                    sys.exit(1)

            zipMeta[key] = val
        try:
            ###New in 4.0
            ###Instead of an arbitrary incrementing ID and version in the path,
            ###We're instead going to be hashing the input / source zip to generate the ID.
            ###This will result in a unique ID for each input dataset, with a very (very very) small chance
            ###of collision, as we'll be retaining the ISO and Boundary Type prefixes.
            ###This will also be compatible with previous versions of gB, as we will retain the use of
            ###an integer - it will just be a hash int instead of arbitray.
            ###Most importantly, users can identify if what we have is the same or different than what they have
            ###based on the ID alone, and we can track changes based on ID.

            row["boundaryID"] = zipMeta['ISO-3166-1 (Alpha-3)'] + "-" + zipMeta["Boundary Type"] + "-" + str(metaHash)
        except:
            row["boundaryID"] = "METADATA ERROR"

        try:
            row["boundaryISO"] = zipMeta['ISO-3166-1 (Alpha-3)']
        except:
            row["boundaryISO"] = "METADATA ERROR"

        try:
            row["boundaryYear"] = zipMeta["Boundary Representative of Year"]
        except:
            row["boundaryYear"] = "METADATA ERROR"

        try:
            row["boundaryType"] = zipMeta["Boundary Type"]
        except:
            row["boundaryType"] = "METADATA ERROR"

        try:
            row["boundarySource-1"] = zipMeta["Source 1"]
        except:
            row["boundarySource-1"] = "METADATA ERROR"

        try:
            row["boundarySource-2"] = zipMeta["Source 2"]
        except:
            row["boundarySource-2"] = "METADATA ERROR"

        try:
            row["boundaryCanonical"] = zipMeta["Canonical Boundary Type Name"]
        except:
            row["boundaryCanonical"] = ""

        try:
            row["boundaryLicense"] = zipMeta["License"]
        except:
            row["boundaryLicense"] = "METADATA ERROR"

        try:
            row["licenseDetail"] = zipMeta["License Notes"]
        except:
            row["licenseDetail"] = "METADATA ERROR"

        try:
            row["licenseSource"] = zipMeta["License Source"]
        except:
            row["licenseSource"] = "METADATA ERROR"

        try:
            row["boundarySourceURL"] = zipMeta["Link to Source Data"]
        except:
            row["boundarySourceURL"] = "METADATA ERROR"

        try:
            row["downloadURL"] = "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/" + str(buildType) + "/" + str(filename)
        except:
            row["downloadURL"] = "METADATA ERROR"
        
        #Build status code
        if(row["status"] == ""):
            if(row["META_requiredChecksPassing"] == True and row["GEOM_requiredChecksPassing"] == True):
                row["status"] = "PASS"
            else:
                row["status"] = "FAIL"
        
        if(row["status"] == "FAIL"):
            #Identify if an issue already exists, and if not create one.
            import github
            import json
            import random
            import time

            #Rate limit for github search api (max 30 requests / minute; running 3 of these scripts simultaneously = 6 sec)
            time.sleep(6)
            #Load in testing environment
            try:
                with open("tmp/accessToken", "r") as f:
                    token = f.read()
            except:
                token = os.environ["GITHUB_TOKEN"]
            
            g = github.Github(token)

            #Github has no "OR" for searching, so a bit of a messy hack here to allow for
            #"ADM0" and "ADM 0"
            likelyIssues = g.search_issues(query=str(row["boundaryISO"]+"+"+row["boundaryType"]+"+"+buildType), repo="wmgeolab/geoBoundaries", state="open")
            issueCount = sum(not issue.pull_request for issue in likelyIssues)
            repo_create = False
            comment_create = False
            if(issueCount == 0):
                admLevel = row["boundaryType"].split("M")[1]
                likelyIssues = g.search_issues(query=str(row["boundaryISO"]+"+'ADM "+str(admLevel)+"'+"+buildType), repo="wmgeolab/geoBoundaries", state="open")
                issueCount = sum(not issue.pull_request for issue in likelyIssues)

            if(issueCount == 0):
                #Search by filename and type, if metadata.txt failed to open at all.
                likelyIssues = g.search_issues(query=str(filename+"+"+str(buildType)), repo="wmgeolab/geoBoundaries", state="open")
                issueCount = sum(not issue.pull_request for issue in likelyIssues)
                 
                

            if(issueCount > 1):
                print("There are currently more than one active issue for this boundary.  Skipping issue creation for now.")
            
            if(issueCount == 0):
                print("Creating issue for " + str(filename)+" "+ buildType)
                repo = g.get_repo("wmgeolab/geoBoundaries")
                issueCreationCount = issueCreationCount + 1
                print("issueCreation:" + str(issueCreationCount))

                wordsForHello = ["Greetings", "Hello", "Hi", "Howdy", "Bonjour", "Beep Boop Beep", "Good Day", "Hello Human"]
                responsestr = random.choice(wordsForHello) + "!  I am the geoBoundary bot, here with a some details on what I need. \n"
                responsestr = responsestr + "I'll print out my logs for you below so you know what's happening! \n"
                responsestr = responsestr + "\n\n \n"
                responsestr = responsestr + json.dumps(row, sort_keys=True, indent=4)
                responsestr = responsestr + "\n\n \n"
                responsestr = responsestr + "====robotid-d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649==== \n"
                responsestr = responsestr + "\n\n"
                
                repo.create_issue(title=str(filename+" "+buildType), body=responsestr)
                repo_create = True

            if(issueCount == 1 and repo_create == False and comment_create == False):
                allCommentText = likelyIssues[0].body
                for i in range(0, likelyIssues[0].get_comments().totalCount):
                    allCommentText = allCommentText + likelyIssues[0].get_comments()[i].body
                if("d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649" not in allCommentText):
                    print("Commenting on issue for " + filename +"+"+buildType)
                    issueCommentCount = issueCommentCount + 1
                    print("issueComment: " + str(issueCommentCount))
                    wordsForHello = ["Greetings", "Hello", "Hi", "Howdy", "Bonjour", "Beep Boop Beep", "Good Day", "Hello Human", "Hola", "Hiya", "Hello There", "Ciao", "Aloha", "What's Poppin'","Salutations","Gidday", "Cheers"]
                    responsestr = random.choice(wordsForHello) + "!  I am the geoBoundary bot, here with a some details on what I need. \n"
                    responsestr = responsestr + "I'll print out my logs for you below so you know what's happening! \n"
                    responsestr = responsestr + "\n\n \n"
                    responsestr = responsestr + json.dumps(row, sort_keys=True, indent=4)
                    responsestr = responsestr + "\n\n \n"
                    responsestr = responsestr + "====robotid-d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649==== \n"
                    responsestr = responsestr + "\n\n"
                    likelyIssues[0].create_comment(responsestr)
                    comment_create = True
                else:
                    print("I have already commented on " + filename +"+"+buildType)
        




        if(row["META_requiredChecksPassing"] == True and row["GEOM_requiredChecksPassing"] == True):

            #Build high level structure
            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/")

            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/")

            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/")

            if not os.path.exists(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/"):
                os.makedirs(os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/")
                
            basePath = os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/"
            
            workingPath = os.path.expanduser("~") + "/working/"
            if not os.path.exists(workingPath):
                os.makedirs(workingPath)


            #Build the files if needed, and all tests are passed.
            jsonOUT = (basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".geojson")
            topoOUT = (basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".topojson")
            shpOUT  = (basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".zip")
            imgOUT  = (basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-PREVIEW.png")
            fullZip = (basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-all.zip")
            inputDataPath = ws["working"] + "/" + ws['zips'][0]

            currentBuild = os.path.getmtime(inputDataPath)
            
            #Get commit from most recent source file.
            sourceQuery = """
                        {
                            repository(owner: \"wmgeolab\", name: \"geoBoundaries\") {
                            object(expression: \"main\") {
                                ... on Commit {
                                blame(path: \"sourceData/"""+buildType+"""/"""+cQuery+"""_"""+typeQuery+""".zip\") {
                                    ranges {
                                    commit {
                                        committedDate
                                    }
                                    }
                                }
                                }
                            }
                            }
                        }
                        """

        
            headers = {"Authorization": "Bearer %s" % APIkey}
            
            request = requests.post('https://api.github.com/graphql', json={'query': sourceQuery}, headers=headers)
            response = request.json()
            

            for i in range(0, len(response["data"]["repository"]["object"]["blame"]["ranges"])):
                curDate = response["data"]["repository"]["object"]["blame"]["ranges"][i]["commit"]["committedDate"]
                if(i == 0):
                    commitDate = curDate
                else:
                    if(commitDate < curDate):
                        commitDate = curDate

           
            print("Building Metadata and HPSCU Geometries for: " + str(fullZip))
            humanDate = datetime.strptime(commitDate.split("T")[0], '%Y-%m-%d')
            row["sourceDataUpdateDate"] = humanDate.strftime('%b %d, %Y')
            row["buildUpdateDate"] = time.strftime('%b %d, %Y')

            #Clean any old items
            if(os.path.isfile(fullZip)):
                shutil.rmtree(basePath)
                os.mkdir(basePath)

            #First, generate the citation and use document
            with open(basePath + "CITATION-AND-USE-geoBoundaries-"+str(buildType)+".txt", "w") as cu:
                cu.write(gbHelpers.citationUse(str(buildType)))

            #Metadata
            #Clean it up by removing our geom and meta checks.
            removeKey = ["status", "META_requiredChecksPassing", "GEOM_requiredChecksPassing", "META_canonicalNameInMeta", "META_licenseImageInZip", "META_yearValid", "META_isoValid","META_boundaryTypeValid", "META_sourceExists", "META_releaseTypeValid", "META_releaseTypeCorrectFolder", "META_licenseValid", "META_licenseSourceExists", "META_dataSourceExists", "GEOM_boundaryNamesColumnExists", "GEOM_boundaryNamesFilledIn", "GEOM_boundaryISOColumnExists", "GEOM_boundaryISOsFilledIn", "GEOM_Topology", "GEOM_Projection"]
            rowMetaOut = {key: row[key] for key in row if key not in removeKey}
            with open(basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.json", "w", encoding="utf-8") as jsonMeta:
                json.dump(rowMetaOut, jsonMeta)

            with open(basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.txt", "w", encoding="utf-8") as textMeta:
                for i in rowMetaOut:
                    textMeta.write(i + " : " + str(rowMetaOut[i]) + "\n")
        
            #Load geometries

            with zipfile.ZipFile(ws["working"] + "/" + ws['zips'][0]) as zF:
                zF.extractall(workingPath)

                geojson = list(filter(lambda x: x[-8:] == '.geojson', zF.namelist()))
                shp = list(filter(lambda x: x[-4:] == '.shp', zF.namelist()))
                geojson = [x for x in geojson if not x.__contains__("MACOS")]
                shp = [x for x in shp if not x.__contains__("MACOS")]
                allShps = geojson + shp

            print(shp)
            print(geojson)
            try:
                dta = geopandas.read_file(workingPath + shp[0])
            except:
                try:
                    dta = geopandas.read_file(workingPath + geojson[0])
                except:
                    print("CRITICAL ERROR: Could not load geometry to build file.")
            

            ####################
            ####################
            #Handle casting to MultiPolygon for Consistency 
            dta["geometry"] = [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in dta["geometry"]]

            ####################
            ####################     
            ####Standardize the Name and ISO columns, if they exist.
            nameC = set(['Name', 'name', 'NAME', 'shapeName', 'shapename', 'SHAPENAME']) 
            nameCol = list(nameC & set(dta.columns))
            if(len(nameCol) == 1):
                dta = dta.rename(columns={nameCol[0]:"shapeName"})
            
            isoC = set(['ISO', 'ISO_code', 'ISO_Code', 'iso', 'shapeISO', 'shapeiso', 'shape_iso']) 
            isoCol = list(isoC & set(dta.columns))
            if(len(isoCol) == 1):
                dta = dta.rename(columns={isoCol[0]:"shapeISO"})

            ####################
            ####################     
            ####Shape IDs.  ID building strategy has changed in gb 4.0.
            ####Previously, an incrementing arbitrary numeric ID was set.
            ####Now, we are hashing the geometry.  Thus, if the geometry doesn't change,
            ####The ID won't either.  This will also be robust across datasets.
            def geomID(geom, metaHash = row["boundaryID"]):
                hashVal = int(hashlib.sha256(str(geom["geometry"]).encode(encoding='UTF-8')).hexdigest(), 16) % 10**8
                return(str(metaHash) + "B" + str(hashVal))

            dta[["shapeID"]] = dta.apply(lambda row: geomID(row), axis=1)
            
            dta[["shapeGroup"]] = row["boundaryISO"]
            dta[["shapeType"]] = row["boundaryType"]
            
            #Output the intermediary geojson without topology corrections
            dta.to_file(workingPath + row["boundaryID"] + ".geoJSON", driver="GeoJSON")
            
            #Write our shapes with self-intersection corrections
            #New in 4.0: we are now snapping to an approximately 1 meter grid.
            #To the surprise of hopefully noone, our products are not suitable for applications which require
            #sub-.1 meter accuracy (true limits will be much higher than this, due to data accuracy).
            write = ("mapshaper-xl 6gb " + workingPath + row["boundaryID"] + ".geoJSON" +
                    " -clean gap-fill-area=500m2 sliver-control=0 snap-interval=.00001 rewind" +
                    " -o format=shapefile " + shpOUT +
                    " -o format=topojson " + topoOUT +
                    " -o format=geojson " + jsonOUT)
            
            os.system(write)                

            dta.boundary.plot(edgecolor="black")
            if(len(row["boundaryCanonical"]) > 1):
                plt.title("geoBoundaries.org - " + buildType + "\n" + row["boundaryISO"] + " " + row["boundaryType"] + "(" + row["boundaryCanonical"] +")" + "\nLast Source Data Update: " + str(row["sourceDataUpdateDate"]) + "\nSource: " + str(row["boundarySource-1"]))
            else:
                plt.title("geoBoundaries.org - " + buildType + "\n" + row["boundaryISO"] + " " + row["boundaryType"] + "\nLast Source Data Update: " + str(row["sourceDataUpdateDate"]) + "\nSource: " + str(row["boundarySource-1"]))
            plt.savefig(imgOUT)

            shutil.make_archive(workingPath + row["boundaryID"], 'zip', basePath)
            shutil.move(workingPath + row["boundaryID"] + ".zip", fullZip)
                

        csvR.append(row)

# Saved CSV as an artifact - TBD if this code stays here, or just log.

try:
    keys = csvR[0].keys()
    with open(os.path.expanduser("~") + "/artifacts/results"+str(buildType)+".csv", "w") as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(csvR)
except:
    print("No CSV log to output.")

try:
    #Copy the log over for an artifact
    os.system("mv " + os.path.expanduser("~") + "/tmp/" + str(buildType) + ".txt" +" " + os.path.expanduser("~") + "/artifacts/log"+str(buildType)+".txt")
except:
    print("No log to output.")


#Copy the tmp directory over to the main repository
try:
    os.system("ls " + ws["working"])
    copy_tree(os.path.expanduser("~") + "/tmp/", ws["working"])
    os.system("ls " + ws["working"])
except:
    print("Nothing to copy")


try:
    if(ws["working"] != "/home/dan/git/geoBoundaries"):
        try:
            os.remove(os.path.expanduser("~")+"/tmp/RESULT.TXT")
        except:
            print("Cleanup skipped.")
        os.system("ls " + ws["working"])

except:
    print("No changes to copy / commit")

if(row["META_requiredChecksPassing"] != True or row["GEOM_requiredChecksPassing"] != True):
    print("At least one check failed.  Stopping build.")
    sys.exit("Either a metadata or Geometry check failed.  Exiting build.")
