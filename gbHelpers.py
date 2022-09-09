import os
import csv
import shutil
import geopandas as gpd
from shapely.geometry import Polygon, MultiPolygon

def initiateWorkspace(check, build = None):
    ws = {}
    if(build != None):
        try:
            ws['working'] = os.environ['GITHUB_WORKSPACE']
            ws['logPath'] = os.path.expanduser("~") + "/tmp/" + str(check) + "_buildStatus.csv"
        except:
            ws['working'] = "/home/dan/git/geoBoundaries"
            ws['logPath'] = os.path.expanduser("~") + "/tmp/" + str(check) + "_buildStatus.csv"

        print("Python WD: " + ws['working'])  
        print("Logging Path: " + str(ws["logPath"]))
        ws['zips'] = []

    else:
        try:
            ws['working'] = os.environ['GITHUB_WORKSPACE']
            ws['changedFiles'] = os.environ['changes'].strip('][').split(',')
            ws['logPath'] = os.path.expanduser("~") + "/tmp/" + str(check) + ".txt"
            ws['zips'] = list(filter(lambda x: x[-4:] == '.zip', ws["changedFiles"]))
        except:
            ws['working'] = "/home/dan/git/geoBoundaries"
            ws['changedFiles'] = ['sourceData/PCN_ADM0.zip']
            ws['logPath'] = os.path.expanduser("~") + "/tmp/" + str(check) + ".txt"
            ws['zips'] = list(filter(lambda x: x[-4:] == '.zip', ws["changedFiles"]))

        print("Python WD: " + ws['working'])  
        print("Python changedFiles: " + str(ws['changedFiles']))
        print("Logging Path: " + str(ws["logPath"]))
        print("Changed Zips Detected: " + str(ws['zips']))

    ws["zipFailures"] = 0
    ws["zipSuccess"] = 0
    ws["zipTotal"] = 0
    ws["checkType"] = check
    return ws

def logWrite(check, line):
    #if(check != "gbAuthoritative" and check != "gbHumanitarian" and check != "gbOpen"):
    print(line)
    with open(os.path.expanduser("~") + "/tmp/" + str(check) + ".txt", "a") as f:
        f.write(line + "\n")

def checkRetrieveLFSFiles(z, workingDir="./"):
    try:
        with open(workingDir + "/.gitattributes") as f:
            lfsList = list(csv.reader(f, delimiter=" "))
        #print(lfsList)
        #print(z)
        lfsFiles = [i[0] for i in lfsList]
        #print(lfsFiles)
        if(z in lfsFiles):
            print("")
            print("--------------------------------")
            print("Downloading LFS File (file > 25mb): " + z)
            os.system('git lfs pull --include=\"' + z +'\"')
            
        else:
            #print("")
            #print("--------------------------------")
            #print("No download from LFS required (file < 25mb): " + z)
            #print("")
            return(0)
    except:
        print("Skipping LFS download; should not be needed for nightly.")

def gbEnvVars(varName, content,mode):
    if(mode == "w"):
        with open(os.path.expanduser("~") + "/tmp/" + varName + ".txt", "w+") as f:
            f.write(content)
        print("Set variable " + str(varName) + " to " + str(content))
    if(mode == "r"):
        with open(os.path.expanduser("~") + "/tmp/" + varName + ".txt", "r") as f:
            return f.read()

def unzipGB(zipObj):
    zipObj.extractall("tmp/")
    if(os.path.exists("tmp/__MACOSX")):
        shutil.rmtree("tmp/__MACOSX")


def citationUse(releaseType):
    citUse = "====================================================\n"
    citUse = citUse + "Citation of the geoBoundaries Data Product\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "www.geoboundaries.org \n"
    citUse = citUse + "geolab.wm.edu \n"
    citUse = citUse + "The geoBoundaries database is made available in a \n"
    citUse = citUse + "variety of software formats to support GIS software programs.\n"
    
    if(releaseType == "gbOpen"):
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
    
    citUse = citUse + "We update geoBoundaries on a yearly cycle, \n"
    citUse = citUse + "with new versions in or around August of each calendar \n"
    citUse = citUse + "year; old versions remain accessible at www.geoboundaries.org. \n"
    citUse = citUse + "The only requirement to use this data is to, with any use, provide\n"
    citUse = citUse + "information on the authors (us), a link to geoboundaries.org or \n"
    citUse = citUse + "our academic citation, and the version of geoBoundaries used. \n"
    citUse = citUse + "Example citations for GeoBoundaries are:  \n"
    citUse = citUse + " \n"
    citUse = citUse + "+++++ General Use Citation +++++\n"
    citUse = citUse + "Please include the term 'geoBoundaries' with a link to \n"
    citUse = citUse + "https://www.geoboundaries.org\n"
    citUse = citUse + " \n"
    citUse = citUse + "+++++ Academic Use Citation +++++++++++\n"
    citUse = citUse + "Runfola D, Anderson A, Baier H, Crittenden M, Dowker E, Fuhrig S, et al. (2020) \n"
    citUse = citUse + "geoBoundaries: A global database of political administrative boundaries. \n"
    citUse = citUse + "PLoS ONE 15(4): e0231866. https://doi.org/10.1371/journal.pone.0231866. \n"
    citUse = citUse + "\n"
    citUse = citUse + "Users using individual boundary files from geoBoundaries should additionally\n"
    citUse = citUse + "ensure that they are citing the sources provided in the metadata for each file.\n"
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Column Definitions\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "boundaryID - A unique ID created for every boundary in the geoBoundaries database by concatenating ISO 3166-1 3 letter country code, boundary level, geoBoundaries version, and an incrementing ID.\n"
    citUse = citUse + "boundaryISO -  The ISO 3166-1 3-letter country codes for each boundary.\n"
    citUse = citUse + "boundaryYear - The year for which a boundary is representative.\n"
    citUse = citUse + "boundaryType - The type of boundary defined (i.e., ADM0 is equivalent to a country border; ADM1 a state.  Levels below ADM1 can vary in definition by country.)\n"
    citUse = citUse + "boundarySource-K - The name of the Kth source for the boundary definition used (with most boundaries having two identified sources).\n"
    citUse = citUse + "boundaryLicense - The specific license the data is released under.\n"
    citUse = citUse + "licenseDetail - Any details necessary for the interpretation or use of the license noted.\n"
    citUse = citUse + "licenseSource - A resolvable URL (checked at the time of data release) declaring the license under which a data product is made available.\n"
    citUse = citUse + "boundarySourceURL -  A resolvable URL (checked at the time of data release) from which source data was retrieved.\n"
    citUse = citUse + "boundaryUpdate - A date encoded following ISO 8601 (Year-Month-Date) describing the last date this boundary was updated, for use in programmatic updating based on new releases.\n"
    citUse = citUse + "downloadURL - A URL from which the geoBoundary can be downloaded.\n"
    citUse = citUse + "shapeID - The boundary ID, followed by the letter `B' and a unique integer for each shape which is a member of that boundary.\n"
    citUse = citUse + "shapeName - The identified name for a given shape.  'None' if not identified.\n"
    citUse = citUse + "shapeGroup - The country or similar organizational group that a shape belongs to, in ISO 3166-1 where relevant.\n"
    citUse = citUse + "shapeType - The type of boundary represented by the shape.\n"
    citUse = citUse + "shapeISO - ISO codes for individual administrative districts, where available.  Where possible, these conform to ISO 3166-2, but this is not guaranteed in all cases. 'None' if not identified.\n"
    citUse = citUse + "boundaryCanonical - Canonical name(s) for the administrative hierarchy represented.  Present where available."
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Reporting Issues or Errors\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "We track issues associated with the geoBoundaries dataset publically,\n"
    citUse = citUse + "and any individual can contribute comments through our github repository:\n"
    citUse = citUse + "https://github.com/wmgeolab/geoBoundaries\n"
    citUse = citUse + " \n"
    citUse = citUse + " \n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "Disclaimer"
    citUse = citUse + "====================================================\n"
    citUse = citUse + "With respect to the works on or made available\n"
    citUse = citUse + "through download from www.geoboundaries.org,\n"
    citUse = citUse + "we make no representations or warranties—express, implied, or statutory—as\n"
    citUse = citUse + "to the validity, accuracy, completeness, or fitness for a particular purpose;\n" 
    citUse = citUse + "nor represent that use of such works would not infringe privately owned rights;\n"
    citUse = citUse + "nor assume any liability resulting from use of such works; and shall in no way\n"
    citUse = citUse + "be liable for any costs, expenses, claims, or demands arising out of use of such works.\n"
    citUse = citUse + "====================================================\n"
    citUse = citUse + " \n"
    citUse = citUse + " \n"
    citUse = citUse + "Thank you for citing your use of geoBoundaries and reporting any issues you find -\n"
    citUse = citUse + "as a non-profit academic project, your citations are what keeps geoBoundaries alive.\n"
    citUse = citUse + "-Dan Runfola (github.com/DanRunfola ; danr@wm.edu)"

    return(citUse)

