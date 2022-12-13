import pandas as pd
import os
import time
import mpi4py
from mpi4py import MPI
import itertools
import math
import glob
import random
import sys

f = "/sciclone/geograd/geoBoundaries/scripts/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
comm_rank = comm.Get_rank()
print(comm_size)


#Run Variables
GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"
LOG_DIR = "/sciclone/geograd/geoBoundaries/logs/gbBuilder/"
TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilder/"
STAT_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderWatch/"
STAGE_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderStage/"
CORE_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderCore/"
#Limits total number of ADM units & resources requested.
TEST = True


with open(STAGE_DIR + "buildStatus", 'w') as f:
    f.write("BUILD HAS COMMENCED.")


#===============
admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
productTypes = ["gbOpen", "gbAuthoritative", "gbHumanitarian"]
#Load in ISOs from master ISO list
countries = pd.read_csv("../dta/iso_3166_1_alpha_3.csv")
isoList = countries["Alpha-3code"].values

licenses = pd.read_csv("../dta/gbLicenses.csv")
licenseList = licenses["license_name"].values

if(TEST == True):
    admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
    productTypes = ["gbOpen"]
    isoList = ["NOR"]

if(MPI.COMM_WORLD.Get_rank() == 0):

    #Clear any existing status information
    with open(STAGE_DIR + "buildStatus", 'w') as f:
        f.write("CLEARING STATUS FROM PREVIOUS RUNS")
    
    statusFiles = glob.glob(STAT_DIR+"*")
    for f in statusFiles:
        os.remove(f)

    with open(STAGE_DIR + "buildStatus", 'w') as f:
            f.write("GENERATING NEW STATUS FILES & JOB LIST")
    jobList = []
    for adm in admTypes:
        for product in productTypes:
            for iso in isoList:
                jobList.append([iso, adm, product])
                with open(STAT_DIR + "_" + iso + "_" + adm + "_" + product, 'w') as f:
                    f.write("P")
    
    #Shuffle the list to mitigate the chance one node gets all the small jobs.
    random.shuffle(jobList)

    with open(STAGE_DIR + "buildStatus", 'w') as f:
            f.write("SENDING TASKS TO NODES")
    

    print("Total Jobs: " + str(len(jobList)))
    #Running on 32GB machines with 10 cores each
    #Want to ensure we have 16GB available for any single process.
    #(2 jobs per node)
    
    nodeCount = comm_size/10
    coresToAllocate = nodeCount * 2
    
    #tasks_per_core = math.ceil(len(jobList)/ (coresToAllocate-1))
    #print(tasks_per_core)
       
    #taskStart = 0
    #taskEnd = tasks_per_core

    #INITIALIZE CHECK FILES
    for core in range(1,(comm_size-1)):
        if((str(core)[-1:] == '4') or (str(core)[-1:] == '5')):
            with open(CORE_DIR + str(core), 'w') as f:
                f.write("D")


    checkExit = False
    currentJob = 0
    maxJob = len(jobList)
    while(checkExit == False):
        errorCount = 0
        skipCount = 0
        STAT_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderWatch/"
        statusFiles = glob.glob(STAT_DIR+"*")

        for core in range(1,(comm_size-1)):
            if((str(core)[-1:] == '4') or (str(core)[-1:] == '5')):
                with open(CORE_DIR + str(core), 'r') as f:
                    stat = f.read()
                if((stat == "D") and (currentJob <= (maxJob-1))):
                    print(jobList)
                    print(currentJob)
                    print(core)
                    chunk = [jobList[currentJob],core]
                    print(chunk)
                    with open(CORE_DIR + str(core), "w") as f:
                        f.write(str(jobList[currentJob]))
                    comm.send(chunk, dest=core, tag=1)
                    currentJob = currentJob + 1
                

        allOutcomes = []
        for f in statusFiles:
            with open(f,"r") as fHandler:
                v = fHandler.read()
                iso = str(f).split("_")[1]
                adm = str(f).split("_")[2]
                product = str(f).split("_")[3]
                if(v == "S"):
                    allOutcomes.append("D")
                    skipCount = skipCount + 1
                elif("E" in v):
                    allOutcomes.append("D")
                    errorCount = errorCount + 1
                    with open(STAGE_DIR + "buildStatus", 'w') as f:
                        f.write("BUILD ERROR.")
                else:
                    allOutcomes.append(v)
            
        percentDone = (allOutcomes.count("D")-skipCount) / (len(allOutcomes)-skipCount)*100

        if(allOutcomes.count("D") == len(allOutcomes)):
            with open(STAGE_DIR + "buildStatus", 'w') as f:
                f.write("BUILD IS COMPLETE.")
            checkExit = True
        else:
            with open(STAGE_DIR + "buildStatus", 'w') as f:
                f.write(str(round(percentDone,2)) + " percent complete (" + str(allOutcomes.count("D")-skipCount) + " of " + str(len(allOutcomes)-skipCount) + ", " + str(skipCount) + " skipped) | BUILD ERRORS: " + str(errorCount))
                    
        time.sleep(1)

else:
    def build(ISO, ADM, product, validISO=isoList, validLicense=licenseList):
        bnd = builder(ISO, ADM, product, GB_DIR, LOG_DIR, TMP_DIR, validISO, licenseList)
        bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.ctime()))   

        def statusUpdate(ISO, ADM, product, code):
            with open(STAT_DIR + "_" + ISO + "_" + ADM + "_" + product, 'w') as f:
                f.write(code)

        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="L")

        bnd.checkExistence()
        if(bnd.existFail == 1):
            return([ISO,ADM,product,"No source data for boundary exists.  Skipping.","S"])

        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="L")    
        validSource = bnd.checkSourceValidity()
        if("ERROR" in validSource):
            return([ISO,ADM,product,validSource,"EV"])
        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="V") 

        metaBuild = bnd.checkBuildTabularMetaData()
        if("ERROR" in metaBuild):
            return([ISO,ADM,product,metaBuild,"EM"])
        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="M") 

        geomChecks = bnd.checkBuildGeometryFiles()
        if("ERROR" in geomChecks):
            return([ISO,ADM,product,geomChecks,"EG"])
        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="G") 

        geomMetaBuild = bnd.calculateGeomMeta()
        if("ERROR" in geomMetaBuild):
            return([ISO, ADM, product, geomMetaBuild, "EB"])
        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="B") 

        saveFiles = bnd.constructFiles()
        if("ERROR" in saveFiles):
            return([ISO, ADM, product, saveFiles,"EC"])
        statusUpdate(ISO=ISO, ADM=ADM, product=product, code="D") 

        
        return([ISO,ADM,product,"Succesfully built.","D"])

    while True:
        layers = comm.recv(source=0, tag=1)
        print("I (" + str(MPI.COMM_WORLD.Get_rank()) + ", "+str(layers[1])+") have received " + str(layers[0]) + " layers to build.")
        ret = []
        core = layers[1]
        l = layers[0]
        o = build(ISO=l[0], ADM=l[1], product=l[2])
        ret.append(o)
        with open(STAT_DIR + "_" + l[0] + "_" + l[1] + "_" + l[2], 'w') as f:
            f.write(o[4])
        with open(CORE_DIR + str(core), "w") as f:
            f.write("D")
        time.sleep(15)

    
    print(ret)
