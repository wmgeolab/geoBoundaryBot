import csv
import hashlib
import json
import os
import random
import shutil
import sys
import time
import zipfile
from datetime import datetime
from distutils.dir_util import copy_tree
from pathlib import Path

import geopandas
import matplotlib.pyplot as plt
import requests
from github import github
from shapely.geometry.multipolygon import MultiPolygon
from shapely.geometry.polygon import Polygon

import gbDataCheck
import gbHelpers
import gbMetaCheck


def promote_to_multipolygon(geometry):
    return [MultiPolygon([feature]) if type(feature) == Polygon else feature for feature in geometry]


def process_file(args, ws, rows, filename):
    bCnt = len(rows) + 1
    print("Processing " + str(filename) + " (boundary " + str(bCnt) + ")")
    row = {}
    row["filename"] = filename
    row["status"] = ""
    row["META_requiredChecksPassing"] = 0
    row["GEOM_requiredChecksPassing"] = 0
    row["presentTargetColumns"] = []
    row["issueCreated"] = 0
    row["issueComment"] = 0
    ws["zipSuccess"] = 0

    ws["zips"] = []
    ws["zips"].append("/sourceData/" + args.buildType + "/" + filename)

    meta, metaHash = check_for_meta_txt(args, ws, row)

    # Check that the meta.txt is passing all checks.
    print("Processing metadata checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
    metaChecks = gbMetaCheck.metaCheck(ws)

    if metaChecks[2] != 1 and args.buildVer != "nightly":
        print(
            "At least one metadata check is failing, so you cannot make a release build.  Try a nightly build first. Here is what we know :"
            + str(metaChecks)
        )

    if args.buildVer == "nightly":
        row["META_requiredChecksPassing"] = bool(metaChecks[2])
        row["META_canonicalNameInMeta"] = bool(metaChecks[0]["canonical"])
        row["META_licenseImageInZip"] = bool(metaChecks[0]["licenseImage"])
        row["META_yearValid"] = bool(metaChecks[1]["year"])
        row["META_isoValid"] = bool(metaChecks[1]["iso"])
        row["META_boundaryTypeValid"] = bool(metaChecks[1]["bType"])
        row["META_sourceExists"] = bool(metaChecks[1]["source"])
        row["META_releaseTypeValid"] = bool(metaChecks[1]["releaseType"])
        row["META_releaseTypeCorrectFolder"] = bool(metaChecks[1]["releaseTypeFolder"])
        row["META_licenseValid"] = bool(metaChecks[1]["license"])
        row["META_licenseSourceExists"] = bool(metaChecks[1]["licenseSource"])
        row["META_dataSourceExists"] = bool(metaChecks[1]["dataSource"])

    # Run the automated geometry checks
    print("Processing geometry checks for " + str(filename) + " (boundary " + str(bCnt) + ")")
    print(ws)
    geomChecks = gbDataCheck.geometryCheck(ws)

    if geomChecks[2] != 1 and args.buildVer != "nightly":
        print(
            "At least one geometry check is failing, so you cannot make a release build.  Try a nightly build first. Here is what we know :"
            + str(geomChecks)
        )
        sys.exit(2)

    if args.buildVer == "nightly":
        row["GEOM_requiredChecksPassing"] = bool(geomChecks[2])
        row["GEOM_boundaryNamesColumnExists"] = bool(geomChecks[0]["bndName"])
        row["GEOM_boundaryNamesFilledIn"] = bool(geomChecks[0]["nameCount"])
        row["GEOM_boundaryISOColumnExists"] = bool(geomChecks[0]["bndISO"])
        row["GEOM_boundaryISOsFilledIn"] = bool(geomChecks[0]["isoCount"])
        row["GEOM_Topology"] = bool(geomChecks[0]["topology"])
        row["GEOM_Projection"] = bool(geomChecks[1]["proj"])

    # Build release columns
    zipMeta = {}
    row["boundaryID"] = "METADATA ERROR"
    row["boundaryISO"] = "METADATA ERROR"
    row["boundaryType"] = "METADATA ERROR"

    for m in meta.splitlines():
        e = m.decode("utf-8").split(":")
        if len(e) > 2:
            e[1] = e[1] + e[2]
        key = e[0].strip()
        try:
            val = e[1].strip()
        except:
            if args.buildVer == "nightly":
                row["status"] = "FAIL"
            else:
                print(
                    "The meta.txt file was not parsed correctly for at least one file.  To make a release build, all checks must pass.  Try running a nightly build first. Exiting."
                )
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
        row["boundaryID"] = zipMeta["ISO-3166-1 (Alpha-3)"] + "-" + zipMeta["Boundary Type"] + "-" + str(metaHash)
    except:
        row["boundaryID"] = "METADATA ERROR"

    try:
        row["boundaryISO"] = zipMeta["ISO-3166-1 (Alpha-3)"]
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
        row["downloadURL"] = (
            "https://github.com/wmgeolab/geoBoundaries/raw/main/releaseData/"
            + str(args.buildType)
            + "/"
            + str(filename)
        )
    except:
        row["downloadURL"] = "METADATA ERROR"

        # while I don't think "METADATA ERROR" is a bad strategy, how do you use this data to inform you? Do you check the outputs manually ?
        # what is the business process if an output file ends up with "METADATA ERROR" in one of the fields?

        # Build status code
    if row["status"] == "":
        if row["META_requiredChecksPassing"] == True and row["GEOM_requiredChecksPassing"] == True:
            row["status"] = "PASS"
        else:
            row["status"] = "FAIL"

    if row["status"] == "FAIL":
        if not args.skip_github:
            json, time = github_bot(args, filename, row)

    if row["META_requiredChecksPassing"] == True and row["GEOM_requiredChecksPassing"] == True:

        # Build high level structure
        release_dir = ws["working"] + "/releaseData/"
        country_dir = release_dir + str(args.buildType) + "/" + str(row["boundaryISO"]) + "/"

        Path(release_dir).mkdir(parents=True, exist_ok=True)
        Path(release_dir + str(args.buildType) + "/").mkdir(parents=True, exist_ok=True)
        Path(country_dir).mkdir(parents=True, exist_ok=True)
        Path(country_dir + str(row["boundaryType"]) + "/").mkdir(parents=True, exist_ok=True)

        basePath = country_dir + str(row["boundaryType"]) + "/"

        workingPath = os.path.expanduser("~") + "/working/"
        Path(workingPath).mkdir(parents=True, exist_ok=True)

        # Build the files if needed, and all tests are passed.
        jsonOUT_simp = (
            basePath
            + "geoBoundaries-"
            + str(row["boundaryISO"])
            + "-"
            + str(row["boundaryType"])
            + "_simplified.geojson"
        )
        topoOUT_simp = (
            basePath
            + "geoBoundaries-"
            + str(row["boundaryISO"])
            + "-"
            + str(row["boundaryType"])
            + "_simplified.topojson"
        )
        shpOUT_simp = (
            basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "_simplified.zip"
        )
        jsonOUT = basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".geojson"
        topoOUT = basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".topojson"
        shpOUT = basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + ".zip"
        imgOUT = basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-PREVIEW.png"
        fullZip = basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-all.zip"
        inputDataPath = ws["working"] + "/" + ws["zips"][0]

        currentBuild = os.path.getmtime(inputDataPath)

        # Get commit from most recent source file.
        sourceQuery = (
            """
                        {
                            repository(owner: \"wmgeolab\", name: \"geoBoundaries\") {
                            object(expression: \"main\") {
                                ... on Commit {
                                blame(path: \"sourceData/"""
            + args.buildType
            + """/"""
            + args.cQuery
            + """_"""
            + args.typeQuery
            + """.zip\") {
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
        )

        headers = {"Authorization": "Bearer %s" % args.APIkey}

        request = requests.post("https://api.github.com/graphql", json={"query": sourceQuery}, headers=headers)
        response = request.json()

        print(sourceQuery)
        for i in range(0, len(response["data"]["repository"]["object"]["blame"]["ranges"])):
            curDate = response["data"]["repository"]["object"]["blame"]["ranges"][i]["commit"]["committedDate"]
            print(curDate)
            print(i)
            if i == 0:
                commitDate = curDate
            else:
                if commitDate < curDate:
                    commitDate = curDate

        print("Building Metadata and HPSCU Geometries for: " + str(fullZip))
        humanDate = datetime.strptime(commitDate.split("T")[0], "%Y-%m-%d")
        row["sourceDataUpdateDate"] = humanDate.strftime("%b %d, %Y")
        row["buildUpdateDate"] = time.strftime("%b %d, %Y")

        # Clean any old items
        if os.path.isfile(fullZip):
            shutil.rmtree(basePath)
            os.mkdir(basePath)

        # First, generate the citation and use document
        with open(basePath + "CITATION-AND-USE-geoBoundaries-" + str(args.buildType) + ".txt", "w") as cu:
            cu.write(gbHelpers.citationUse(str(args.buildType)))

        # Metadata
        # Clean it up by removing our geom and meta checks.
        removeKey = [
            "status",
            "META_requiredChecksPassing",
            "GEOM_requiredChecksPassing",
            "META_canonicalNameInMeta",
            "META_licenseImageInZip",
            "META_yearValid",
            "META_isoValid",
            "META_boundaryTypeValid",
            "META_sourceExists",
            "META_releaseTypeValid",
            "META_releaseTypeCorrectFolder",
            "META_licenseValid",
            "META_licenseSourceExists",
            "META_dataSourceExists",
            "GEOM_boundaryNamesColumnExists",
            "GEOM_boundaryNamesFilledIn",
            "GEOM_boundaryISOColumnExists",
            "GEOM_boundaryISOsFilledIn",
            "GEOM_Topology",
            "GEOM_Projection",
        ]
        rowMetaOut = {key: row[key] for key in row if key not in removeKey}
        with open(
            basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.json",
            "w",
            encoding="utf-8",
        ) as jsonMeta:
            json.dump(rowMetaOut, jsonMeta)

        with open(
            basePath + "geoBoundaries-" + str(row["boundaryISO"]) + "-" + str(row["boundaryType"]) + "-metaData.txt",
            "w",
            encoding="utf-8",
        ) as textMeta:
            for i in rowMetaOut:
                textMeta.write(i + " : " + str(rowMetaOut[i]) + "\n")

            # Load geometries

        with zipfile.ZipFile(ws["working"] + "/" + ws["zips"][0]) as zF:
            zF.extractall(workingPath)

            geojson = list(filter(lambda x: x[-8:] == ".geojson", zF.namelist()))
            shp = list(filter(lambda x: x[-4:] == ".shp", zF.namelist()))
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

        dta["geometry"] = promote_to_multipolygon(dta["geometry"])  # for consistency

        ####################
        ####################
        ####Standardize the Name and ISO columns, if they exist.
        nameC = set(["Name", "name", "NAME", "shapeName", "shapename", "SHAPENAME"])
        nameCol = list(nameC & set(dta.columns))
        if len(nameCol) == 1:
            dta = dta.rename(columns={nameCol[0]: "shapeName"})

        if "shapeName" in dta:
            row["presentTargetColumns"].append("shapeName")

        isoC = set(["ISO", "ISO_code", "ISO_Code", "ISO_CODE", "iso", "shapeISO", "shapeiso", "shape_iso"])
        isoCol = list(isoC & set(dta.columns))
        if len(isoCol) == 1:
            dta = dta.rename(columns={isoCol[0]: "shapeISO"})

        # what is the distinction between boundaryISO and shapeISO?

        ####################
        ####################
        ####Shape IDs.  ID building strategy has changed in gb 4.0.
        ####Previously, an incrementing arbitrary numeric ID was set.
        ####Now, we are hashing the geometry.  Thus, if the geometry doesn't change,
        ####The ID won't either.  This will also be robust across datasets.

        def geomID(geom, metaHash=row["boundaryID"]):
            hashVal = int(hashlib.sha256(str(geom["geometry"]).encode(encoding="UTF-8")).hexdigest(), 16) % 10 ** 8
            return str(metaHash) + "B" + str(hashVal)

        dta[["shapeID"]] = dta.apply(lambda row: geomID(row), axis=1)

        dta[["shapeGroup"]] = row["boundaryISO"]
        dta[["shapeType"]] = row["boundaryType"]

        # Note: Some metadata is calculated in the overall CSV build, and so is not included in the default metadata files here.
        # We may want to revisit this at some date in the future.

        # Output the intermediary geojson without topology corrections
        dta.to_file(workingPath + row["boundaryID"] + ".geoJSON", driver="GeoJSON")

        # Write our shapes with self-intersection corrections
        # New in 4.0: we are now snapping to an approximately 1 meter grid.
        # To the surprise of hopefully noone, our products are not suitable for applications which require
        # sub-.1 meter accuracy (true limits will be much higher than this, due to data accuracy).
        write = (
            "mapshaper-xl 6gb "
            + workingPath
            + row["boundaryID"]
            + ".geoJSON"
            + " -clean gap-fill-area=500m2 snap-interval=.00001"
            + " -o format=shapefile "
            + shpOUT
            + " -o format=topojson "
            + topoOUT
            + " -o format=geojson "
            + jsonOUT
        )

        os.system(write)

        # Do a second write, this time with simplification.
        # Simplification attempts to keep around 100-meter resolution along boundaries.
        write = (
            "mapshaper-xl 6gb "
            + workingPath
            + row["boundaryID"]
            + ".geoJSON"
            + " -simplify dp interval=100 keep-shapes"
            + " -clean gap-fill-area=500m2 snap-interval=.00001"
            + " -o format=shapefile "
            + shpOUT_simp
            + " -o format=topojson "
            + topoOUT_simp
            + " -o format=geojson "
            + jsonOUT_simp
        )

        os.system(write)

        dta.boundary.plot(edgecolor="black")
        if len(row["boundaryCanonical"]) > 1:
            plt.title(
                "geoBoundaries.org - "
                + args.buildType
                + "\n"
                + row["boundaryISO"]
                + " "
                + row["boundaryType"].upper().replace(" ", "")
                + "("
                + row["boundaryCanonical"]
                + ")"
                + "\nLast Source Data Update: "
                + str(row["sourceDataUpdateDate"])
                + "\nSource: "
                + str(row["boundarySource-1"])
            )
        else:
            plt.title(
                "geoBoundaries.org - "
                + args.buildType
                + "\n"
                + row["boundaryISO"]
                + " "
                + row["boundaryType"].upper().replace(" ", "")
                + "\nLast Source Data Update: "
                + str(row["sourceDataUpdateDate"])
                + "\nSource: "
                + str(row["boundarySource-1"])
            )
        plt.savefig(imgOUT)

        shutil.make_archive(workingPath + row["boundaryID"], "zip", basePath)
        shutil.move(workingPath + row["boundaryID"] + ".zip", fullZip)

    return row


def check_for_meta_txt(args, ws, row):
    try:
        with zipfile.ZipFile(ws["working"] + "/" + ws["zips"][0]) as zF:
            meta = zF.read("meta.txt")

        m = hashlib.sha256()
        chunkSize = 8192
        with open(ws["working"] + "/" + ws["zips"][0], "rb") as zF:
            while True:
                chunk = zF.read(chunkSize)
                if len(chunk):
                    m.update(chunk)
                else:
                    break
            # 8 digit modulo on the hash.  Won't guarantee unique,
            # but as this is per ADM/ISO, collision is very (very) unlikely.
            metaHash = int(m.hexdigest(), 16) % 10 ** 8
            print(metaHash)

    except:
        if args.buildVer == "nightly":
            row["status"] = "FAIL"
        else:
            print(
                "No meta.txt in at least one file.  To make a release build, all checks must pass.  Try running a nightly build first. Exiting."
            )
            sys.exit(1)
    return meta, metaHash


def github_bot(args, rows, row):
    # identify if an issue already exists, and if not create one.

    # Rate limit for github search api (max 30 requests / minute; running 3 of these scripts simultaneously = 6 sec)
    time.sleep(6)
    # Load in testing environment
    try:
        with open("tmp/accessToken", "r") as f:
            token = f.read()
    except:
        token = os.environ["GITHUB_TOKEN"]

    g = github.Github(token)

    # Github has no "OR" for searching, so a bit of a messy hack here to allow for
    # "ADM0" and "ADM 0"
    likelyIssues = g.search_issues(
        query=str(row["boundaryISO"] + "+" + row["boundaryType"] + "+" + args.buildType),
        repo="wmgeolab/geoBoundaries",
        state="open",
    )
    issueCount = sum(not issue.pull_request for issue in likelyIssues)
    repo_create = False
    comment_create = False
    if issueCount == 0:
        admLevel = row["boundaryType"].split("M")[1]
        likelyIssues = g.search_issues(
            query=str(row["boundaryISO"] + "+'ADM " + str(admLevel) + "'+" + args.buildType),
            repo="wmgeolab/geoBoundaries",
            state="open",
        )
        issueCount = sum(not issue.pull_request for issue in likelyIssues)

    if issueCount == 0:
        # Search by filename and type, if metadata.txt failed to open at all.
        likelyIssues = g.search_issues(
            query=str(row["filename"] + "+" + str(args.buildType)), repo="wmgeolab/geoBoundaries", state="open"
        )
        issueCount = sum(not issue.pull_request for issue in likelyIssues)

    if issueCount > 1:
        print("There are currently more than one active issue for this boundary.  Skipping issue creation for now.")

    if issueCount == 0:
        print("Creating issue for " + str(row["filename"]) + " " + args.buildType)
        repo = g.get_repo("wmgeolab/geoBoundaries")
        row["issueCreated"] = True
        issues_created = len(list(filter(lambda x: x["issueCreated"], rows))) + 1
        print("issueCreation:" + str(issues_created))

        wordsForHello = ["Greetings", "Hello", "Hi", "Howdy", "Bonjour", "Beep Boop Beep", "Good Day", "Hello Human"]

        repo.create_issue(
            title=str(row["filename"] + " " + args.buildType),
            body=random.choice(wordsForHello)
            + "!  I am the geoBoundary bot, here with a some details on what I need. \n"
            + "I'll print out my logs for you below so you know what's happening! \n"
            + "\n\n \n"
            + json.dumps(row, sort_keys=True, indent=4)
            + "\n\n \n"
            + "====robotid-d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649==== \n"
            + "\n\n",
        )
        repo_create = True

    if issueCount == 1 and repo_create == False and comment_create == False:
        allCommentText = likelyIssues[0].body
        for i in range(0, likelyIssues[0].get_comments().totalCount):
            allCommentText = allCommentText + likelyIssues[0].get_comments()[i].body
        if "d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649" not in allCommentText:
            print("Commenting on issue for " + row["filename"] + "+" + args.buildType)
            row["issueCommented"] = True
            issues_commented = len(list(filter(lambda x: x["issueCommented"], rows))) + 1
            print("issueComment: " + str(issues_commented))
            wordsForHello = [
                "Greetings",
                "Hello",
                "Hi",
                "Howdy",
                "Bonjour",
                "Beep Boop Beep",
                "Good Day",
                "Hello Human",
                "Hola",
                "Hiya",
                "Hello There",
                "Ciao",
                "Aloha",
                "What's Poppin'",
                "Salutations",
                "Gidday",
                "Cheers",
            ]

            likelyIssues[0].create_comment(
                random.choice(wordsForHello)
                + "!  I am the geoBoundary bot, here with a some details on what I need. \n"
                + "I'll print out my logs for you below so you know what's happening! \n"
                + "\n\n \n"
                + json.dumps(row, sort_keys=True, indent=4)
                + "\n\n \n"
                + "====robotid-d7329e7104s40t927830R028o9327y372h87u910m197a9472n2837s649==== \n"
                + "\n\n"
            )
            comment_create = True
        else:
            print("I have already commented on " + row["filename"] + "+" + args.buildType)

    return json, time


def build_data(args, ws):
    rows = []
    for (path, dirname, filenames) in os.walk(ws["working"] + "/sourceData/" + args.buildType + "/"):
        selFiles = []
        for i in args.cQuery.split(","):
            selFiles.append([x for x in filenames if x.startswith(i + "_" + args.typeQuery)])
        filesToProcess = [item for sublist in selFiles for item in sublist]
        print(filesToProcess)
        for filename in filesToProcess:
            rows.append(process_file(args, ws, rows, filename))

    return rows


def main(args):
    log = gbHelpers.argparse_log(args)

    ws = gbHelpers.initiateWorkspace(args.buildType, build=True)
    print(ws)

    csvR = build_data(args, ws)

    # Saved CSV as an artifact - TBD if this code stays here, or just log.
    keys = csvR[0].keys()
    with open(os.path.expanduser("~") + "/artifacts/results" + str(args.buildType) + ".csv", "w") as f:
        writer = csv.DictWriter(f, keys)
        writer.writeheader()
        writer.writerows(csvR)

    # Copy the log as an artifact
    shutil.move(
        os.path.expanduser("~") + "/tmp/" + str(args.buildType) + ".txt",
        os.path.expanduser("~") + "/artifacts/log" + str(args.buildType) + ".txt",
    )

    files_with_shapeName = list(filter(lambda x: "shapeName" in x["presentTargetColumns"], csvR))
    print(len(files_with_shapeName), " of ", len(csvR))

    print(list(map(lambda x: x["filename"], files_with_shapeName)))


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("buildType")
    parser.add_argument("buildVer")
    parser.add_argument("cQuery")
    parser.add_argument("typeQuery")
    if not '-skip-github' in str(sys.argv):
        parser.add_argument("APIkey")
    parser.add_argument("-v", "--verbose", action="count", default=0)
    parser.add_argument("-skip-github", "--skip-github", action="store_true")
    args = parser.parse_args()

    main(args)
