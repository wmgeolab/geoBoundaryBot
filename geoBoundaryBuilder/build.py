import pandas as pd
import os
import time
import mpi4py
from mpi4py import MPI
import itertools
import math
import glob
f = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
comm_rank = comm.Get_rank()
print(comm_size)


#Run Variables
GB_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesDev/"
LOG_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/logs/"
TMP_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesTmp/"
STAT_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesStat/"
#Limits total number of ADM units & resources requested.
TEST = True

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
    countries = pd.read_csv("../dta/iso_3166_1_alpha_3.csv")
    isoList = countries["Alpha-3code"].values
    #isoList = ["NOR"]

if(MPI.COMM_WORLD.Get_rank() == 0):

    #Clear any existing status information
    statusFiles = glob.glob(STAT_DIR+"*")
    for f in statusFiles:
        os.remove(f)

    jobList = []
    for adm in admTypes:
        for product in productTypes:
            for iso in isoList:
                jobList.append([iso, adm, product])
                with open(STAT_DIR + "_" + iso + "_" + adm + "_" + product, 'w') as f:
                    f.write("P")
                

    print("Total Jobs: " + str(len(jobList)))

    #Running on 32GB machines with 10 cores each
    #Want to ensure we have 16GB available for any single process.
    #(2 jobs per node)
    
    nodeCount = comm_size/10
    coresToAllocate = nodeCount * 2
    
    tasks_per_core = math.ceil(len(jobList)/ (coresToAllocate-1))
    print(tasks_per_core)
    
    taskStart = 0
    taskEnd = tasks_per_core
    for core in range(1,(comm_size-1)):
        if((str(core)[-1:] == '4') or (str(core)[-1:] == '5')):
            chunk = jobList[taskStart:taskEnd]
            comm.send(chunk, dest=core, tag=1)
            taskStart = taskStart + tasks_per_core
            taskEnd = taskEnd + tasks_per_core

else:
    def build(ISO, ADM, product, validISO=isoList, validLicense=licenseList):
        bnd = builder(ISO, ADM, product, GB_DIR, LOG_DIR, TMP_DIR, validISO, licenseList)
        bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.ctime()))   

        bnd.checkExistence()
        if(bnd.existFail == 1):
            return([ISO,ADM,product,"No source data for boundary exists.  Skipping.","D"])
            
        validSource = bnd.checkSourceValidity()
        if("ERROR" in validSource):
            return([ISO,ADM,product,validSource,"V"])

        metaBuild = bnd.checkBuildTabularMetaData()
        if("ERROR" in metaBuild):
            return([ISO,ADM,product,metaBuild,"M"])

        geomChecks = bnd.checkBuildGeometryFiles()
        if("ERROR" in geomChecks):
            return([ISO,ADM,product,geomChecks,"G"])

        saveFiles = bnd.constructFiles()
        if("ERROR" in saveFiles):
            return([ISO, ADM, product, saveFiles,"C"])

        
        return([ISO,ADM,product,"Succesfully built.","D"])

    layers = comm.recv(source=0, tag=1)
    print("I (" + str(MPI.COMM_WORLD.Get_rank()) + ") have received " + str(layers) + " layers to build.")
    ret = []
    for l in layers:
        o = build(ISO=l[0], ADM=l[1], product=l[2])
        ret.append(o)
        with open(STAT_DIR + "_" + l[0] + "_" + l[1] + "_" + l[2], 'w') as f:
            f.write(o[4])

    
    print(ret)
