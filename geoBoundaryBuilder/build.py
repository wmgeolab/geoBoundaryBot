import pandas as pd
import os
import time
import mpi4py
from mpi4py import MPI
import itertools
import math
f = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

comm = MPI.COMM_WORLD
comm_size = comm.Get_size()
comm_rank = comm.Get_rank()
print(comm_size)


#Run Variables
GB_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesDev/geoBoundaries/"
LOG_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/logs/"
TMP_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesTmp/"
#Limits total number of ADM units & resources requested.
TEST = True

#===============
admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
productTypes = ["geoBoundaries", "UN_SALB", "UN_OCHA"]
#Load in ISOs from master ISO list
countries = pd.read_csv("../dta/iso_3166_1_alpha_3.csv")
isoList = countries["Alpha-3code"].values

licenses = pd.read_csv("../dta/gbLicenses.csv")
licenseList = licenses["license_name"].values

if(TEST == True):
    admTypes = ["ADM0","ADM1","ADM2","ADM3"]
    productTypes = ["geoBoundaries"]
    isoList = ["USA", "MEX", "CAN", "FRA","AUS","BRA","UKR","NOR"]

if(MPI.COMM_WORLD.Get_rank() == 0):

    jobList = []
    for adm in admTypes:
        for product in productTypes:
            for iso in isoList:
                jobList.append([iso, adm, product])
                

    print("Total Jobs: " + str(len(jobList)))

    tasks_per_core = math.ceil(len(jobList)/ (comm_size-1))
    print(tasks_per_core)
    
    taskStart = 0
    taskEnd = tasks_per_core
    for core in range(1,(comm_size-1)):
        chunk = jobList[taskStart:taskEnd]
        comm.send(chunk, dest=core, tag=1)
        taskStart = taskStart + tasks_per_core
        taskEnd = taskEnd + tasks_per_core

else:
    def build(ISO, ADM, product, validISO=isoList, validLicense=licenseList):
        bnd = builder(ISO, ADM, product, GB_DIR, LOG_DIR, TMP_DIR, validISO, licenseList)
        bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.time()))   

        bnd.checkExistence()
        if(bnd.existFail == 1):
            return([ISO,ADM,product,"No source data for boundary exists.  Skipping."])
            
        validSource = bnd.checkSourceValidity()
        if("ERROR" in validSource):
            return([ISO,ADM,product,validSource])

        metaBuild = bnd.buildTabularMetaData()
        if("ERROR" in metaBuild):
            return([ISO,ADM,product,metaBuild])
        
        return([ISO,ADM,product,"Succesfully built."])


    layers = comm.recv(source=0, tag=1)
    print("I (" + str(MPI.COMM_WORLD.Get_rank()) + ") have received " + str(layers) + " layers to build.")
    ret = []
    for l in layers:
        ret.append(build(ISO=l[0], ADM=l[1], product=l[2]))
    
    print(ret)
