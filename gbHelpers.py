import csv
import logging
import os
import shutil
from subprocess import PIPE, run

import geopandas as gpd
from rich import inspect
from rich.logging import RichHandler
from shapely.geometry import MultiPolygon, Polygon


def initiateWorkspace(check, build=None):
    ws = {}
    if build != None:
        ws["working"] = (
            os.environ.get("GITHUB_WORKSPACE") or os.environ.get("GEOBOUNDARIES_DIR") or "/home/dan/git/geoBoundaries"
        )
        ws["logPath"] = os.path.expanduser("~") + "/tmp/" + str(check) + "_buildStatus.csv"
        ws["zips"] = []

    else:
        try:
            ws["working"] = os.environ["GITHUB_WORKSPACE"]
            ws["changedFiles"] = os.environ["changes"].strip("][").split(",")
            ws["logPath"] = os.path.expanduser("~") + "/tmp/" + str(check) + ".txt"
            ws["zips"] = list(filter(lambda x: x[-4:] == ".zip", ws["changedFiles"]))
        except:
            ws["working"] = "/home/dan/git/geoBoundaries"
            ws["changedFiles"] = ["sourceData/gbOpen/ARE_ADM1.zip", "sourceData/gbOpen/QAT_ADM0.zip"]
            ws["logPath"] = os.path.expanduser("~") + "/tmp/" + str(check) + ".txt"
            ws["zips"] = list(filter(lambda x: x[-4:] == ".zip", ws["changedFiles"]))

        print("Python changedFiles: " + str(ws["changedFiles"]))
        print("Changed Zips Detected: " + str(ws["zips"]))

    print("Python WD: " + ws["working"])
    print("Logging Path: " + str(ws["logPath"]))

    ws["zipFailures"] = 0
    ws["zipSuccess"] = 0
    ws["zipTotal"] = 0
    ws["checkType"] = check
    return ws


def logWrite(check, line):
    # if(check != "gbAuthoritative" and check != "gbHumanitarian" and check != "gbOpen"):
    print(line)
    with open(os.path.expanduser("~") + "/tmp/" + str(check) + ".txt", "a") as f:
        f.write(line + "\n")


def checkRetrieveLFSFiles(z, workingDir="./"):
    try:
        with open(workingDir + "/.gitattributes") as f:
            lfsList = list(csv.reader(f, delimiter=" "))
        # print(lfsList)
        # print(z)
        lfsFiles = [i[0] for i in lfsList]
        # print(lfsFiles)
        if z in lfsFiles:
            print("")
            print("--------------------------------")
            print("Downloading LFS File (file > 25mb): " + z)
            os.system('git lfs pull --include="' + z + '"')

        else:
            # print("")
            # print("--------------------------------")
            # print("No download from LFS required (file < 25mb): " + z)
            # print("")
            return 0
    except:
        print("Skipping LFS download; should not be needed for nightly.")


def gbEnvVars(varName, content, mode):
    if mode == "w":
        with open(os.path.expanduser("~") + "/tmp/" + varName + ".txt", "w+") as f:
            f.write(content)
        print("Set variable " + str(varName) + " to " + str(content))
    if mode == "r":
        with open(os.path.expanduser("~") + "/tmp/" + varName + ".txt", "r") as f:
            return f.read()


def unzipGB(zipObj):
    zipObj.extractall("tmp/")
    if os.path.exists("tmp/__MACOSX"):
        shutil.rmtree("tmp/__MACOSX")


def citationUse(releaseType):
    citUse = "====================================================\n"
    citUse = citUse + "Citation of the geoBoundaries Data Product\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "www.geoboundaries.org \n"
    citUse = citUse + "geolab.wm.edu \n"
    citUse = citUse + "The geoBoundaries database is made available in a \n"
    citUse = citUse + "variety of software formats to support GIS software programs.\n"

    if releaseType == "gbOpen":
        citUse = citUse + "This file is a part of the geoBoundaries Open Database \n"
        citUse = citUse + "(gbOpen).  All boundaries in this database are open and \n"
        citUse = citUse + "redistributable, and are released alongside extensive metadata \n"
        citUse = citUse + "and licence information to help inform end users. \n"

    else:
        citUse = citUse + "This file is a part of a geoBoundaries Mixed Database. \n"
        citUse = citUse + "All boundaries in this database are \n"
        citUse = citUse + "redistributable, and are released alongside extensive metadata \n"
        citUse = citUse + "and licence information to help inform end users. \n"
        citUse = citUse + "Unlike data provided in the geoBoundaries Open Database, \n"
        citUse = citUse + "information in this database may have restrictions on (for example) \n"
        citUse = citUse + "commercial use.  Users should carefully read each license to ensure they are \n"
        citUse = citUse + "not violating the terms of an individual layer for any non-private uses. \n"

    geoBoundariesInfo = """We update geoBoundaries on a yearly cycle, \n
with new versions in or around August of each calendar \n
year; old versions remain accessible at www.geoboundaries.org. \n
The only requirement to use this data is to, with any use, provide\n
information on the authors (us), a link to geoboundaries.org or \n
our academic citation, and the version of geoBoundaries used. \n
Example citations for GeoBoundaries are:  \n
 \n
+++++ General Use Citation +++++\n
Please include the term 'geoBoundaries' with a link to \n
https://www.geoboundaries.org\n
 \n
+++++ Academic Use Citation +++++++++++\n
Runfola D, Anderson A, Baier H, Crittenden M, Dowker E, Fuhrig S, et al. (2020) \n
geoBoundaries: A global database of political administrative boundaries. \n
PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866. \n
\n
Users using individual boundary files from geoBoundaries should additionally\n
ensure that they are citing the sources provided in the metadata for each file.\n
 \n
====================================================\n
Column Definitions\n
====================================================\n
boundaryID - A unique ID created for every boundary in the geoBoundaries database by concatenating ISO 3166-1 3 letter country code, boundary level, geoBoundaries version, and an incrementing ID.\n
boundaryISO -  The ISO 3166-1 3-letter country codes for each boundary.\n
boundaryYear - The year for which a boundary is representative.\n
boundaryType - The type of boundary defined (i.e., ADM0 is equivalent to a country border; ADM1 a state.  Levels below ADM1 can vary in definition by country.)\n
boundarySource-K - The name of the Kth source for the boundary definition used (with most boundaries having two identified sources).\n
boundaryLicense - The specific license the data is released under.\n
licenseDetail - Any details necessary for the interpretation or use of the license noted.\n
licenseSource - A resolvable URL (checked at the time of data release) declaring the license under which a data product is made available.\n
boundarySourceURL -  A resolvable URL (checked at the time of data release) from which source data was retrieved.\n
boundaryUpdate - A date encoded following ISO 8601 (Year-Month-Date) describing the last date this boundary was updated, for use in programmatic updating based on new releases.\n
downloadURL - A URL from which the geoBoundary can be downloaded.\n
shapeID - The boundary ID, followed by the letter `B' and a unique integer for each shape which is a member of that boundary.\n
shapeName - The identified name for a given shape.  'None' if not identified.\n
shapeGroup - The country or similar organizational group that a shape belongs to, in ISO 3166-1 where relevant.\n
shapeType - The type of boundary represented by the shape.\n
shapeISO - ISO codes for individual administrative districts, where available.  Where possible, these conform to ISO 3166-2, but this is not guaranteed in all cases. 'None' if not identified.\n
boundaryCanonical - Canonical name(s) for the administrative hierarchy represented.  Present where available.
 \n
====================================================\n
Reporting Issues or Errors\n
====================================================\n
We track issues associated with the geoBoundaries dataset publically,\n
and any individual can contribute comments through our github repository:\n
https://github.com/wmgeolab/geoBoundaries\n
 \n
 \n
====================================================\n
Disclaimer
====================================================\n
With respect to the works on or made available\n
through download from www.geoboundaries.org,\n
we make no representations or warranties—express, implied, or statutory—as\n
to the validity, accuracy, completeness, or fitness for a particular purpose;\n"
nor represent that use of such works would not infringe privately owned rights;\n
nor assume any liability resulting from use of such works; and shall in no way\n
be liable for any costs, expenses, claims, or demands arising out of use of such works.\n
====================================================\n
 \n
 \n
Thank you for citing your use of geoBoundaries and reporting any issues you find -\n
as a non-profit academic project, your citations are what keeps geoBoundaries alive.\n
-Dan Runfola (github.com/DanRunfola ; danr@wm.edu)"""
    citUse = citUse + geoBoundariesInfo

    return citUse


def cmd(command, **kwargs):
    log = logging.getLogger()
    r = run(command, stdout=PIPE, stderr=PIPE, universal_newlines=True, shell=True, **kwargs)
    log.debug(r.args)
    if r.returncode != 0:
        log.error(f"process exited with returncode {r.returncode}")
    log.info(r.stdout.strip())
    log.error(r.stderr.strip())
    return r


def argparse_log(args):
    print(args)
    log_levels = [logging.WARNING, logging.INFO, logging.DEBUG]
    logging.basicConfig(
        level=log_levels[min(len(log_levels) - 1, args.verbose)],
        format="%(message)s",
        datefmt="[%X]",
        handlers=[RichHandler()],
    )
    log = logging.getLogger()
    return log
