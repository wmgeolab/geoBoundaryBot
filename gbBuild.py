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
countries = str(sys.argv[4])
ws = gbHelpers.initiateWorkspace(buildType, build = True)
print(ws)
csvR = []
bCnt = 0
issueCreationCount = 0
issueCommentCount = 0


for (path, dirname, filenames) in os.walk(ws["working"] + "/sourceData/" + buildType + "/"):
    selFiles = []
    for i in countries.split(","):
        selFiles.append([x for x in filenames if x.startswith(i)])
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
        except:
            if(buildVer == "dev"):
                row["status"] = "FAIL"
            else:
                print("No meta.txt in at least one file.  To make a release build, all checks must pass.  Try running a dev build first. Exiting.")
                sys.exit(1)
        
        #Check that the meta.txt is passing all checks.
        print("Processing metadata checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
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
        print("Processing geometry checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
        print(ws)
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
        row["boundaryID"] = "METADATA ERROR"
        row["boundaryISO"] = "METADATA ERROR"
        row["boundaryType"] = "METADATA ERROR"

        for m in meta.splitlines():
            e = m.decode("latin1").split(":")
            if(len(e) > 2):
                e[1] = e[1] + e[2]
            key = e[0].strip()
            try:
                val = e[1].strip()
            except:
                if(buildVer == "dev"):
                    row["status"] = "FAIL"
                else:
                    print("The meta.txt file was not parsed correctly for at least one file.  To make a release build, all checks must pass.  Try running a dev build first. Exiting.")
                    sys.exit(1)

            zipMeta[key] = val
        try:
            row["boundaryID"] = zipMeta['ISO-3166-1 (Alpha-3)'] + "-" + zipMeta["Boundary Type"] + "-" + buildVer
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
            row["boundarySource_1"] = zipMeta["Source 1"]
        except:
            row["boundarySource_1"] = "METADATA ERROR"

        try:
            row["boundarySource_2"] = zipMeta["Source 2"]
        except:
            row["boundarySource_2"] = "METADATA ERROR"

        try:
            row["boundaryCanonical"] = zipMeta["Canonical Boundary Type Name"]
        except:
            row["boundaryCanonical"] = "METADATA ERROR"

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
            row["downloadURL"] = "https://github.com/wmgeolab/gbRelease/raw/master/releaseData/" + str(buildType) + "/" + str(filename)
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
            likelyIssues = g.search_issues(query=str(row["boundaryISO"]+"+"+row["boundaryType"]+"+"+buildType), repo="wmgeolab/gbRelease", state="open")
            issueCount = sum(not issue.pull_request for issue in likelyIssues)
            repo_create = False
            comment_create = False
            if(issueCount == 0):
                admLevel = row["boundaryType"].split("M")[1]
                likelyIssues = g.search_issues(query=str(row["boundaryISO"]+"+'ADM "+str(admLevel)+"'+"+buildType), repo="wmgeolab/gbRelease", state="open")
                issueCount = sum(not issue.pull_request for issue in likelyIssues)

            if(issueCount == 0):
                #Search by filename and type, if metadata.txt failed to open at all.
                likelyIssues = g.search_issues(query=str(filename+"+"+str(buildType)), repo="wmgeolab/gbRelease", state="open")
                issueCount = sum(not issue.pull_request for issue in likelyIssues)
                 
                

            if(issueCount > 1):
                print("There are currently more than one active issue for this boundary.  Skipping issue creation for now.")
            
            if(issueCount == 0):
                print("Creating issue for " + str(filename)+" "+ buildType)
                repo = g.get_repo("wmgeolab/gbRelease")
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

            #Only fill in the folder structure if data is ready to ship
            
            if(row["META_requiredChecksPassing"] == True and row["GEOM_requiredChecksPassing"] == True):
                basePath = os.path.expanduser("~") + "/tmp/releaseData/" + str(buildType) + "/" + str(row["boundaryISO"]) + "/" + str(row["boundaryType"]) + "/"

                #First, generate the citation and use document
                with open(basePath + "CITATION-AND-USE-geoBoundaries-"+str(buildType)+".txt", "w") as cu:
                    cu.write(gbHelpers.citationUse(str(buildType)))
                
                #with open("geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.json"):



            #Build JSON and TXT meta
            
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

#Build the CSV - if fullbuild = False, this is all that gets pushed.  Saved as an artifact.
keys = csvR[0].keys()
with open(os.path.expanduser("~") + "artifacts/results.csv", "w") as f:
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
