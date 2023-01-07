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

#Limits total number of layers to build according to the below parameters if enabled.
limitAdmTypes = False#["ADM0","ADM1","ADM2"]
limitProductTypes = False#["gbAuthoritative", "gbOpen"]
limitISO = False#["DJI","CHL","MKD","FIN","PHL","URY","GRC","LCA","YEM"]#False#["IDN", "LAO"]


#===============
with open(STAGE_DIR + "buildStatus", 'w') as f:
    f.write("BUILD HAS COMMENCED.")

if(limitAdmTypes == False):
    admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
else:
    admTypes = limitAdmTypes

if(limitProductTypes == False):
    productTypes = ["gbOpen", "gbAuthoritative", "gbHumanitarian"]
else:
    productTypes = limitProductTypes

if(limitISO == False):
    pass
else:
    isoList = limitISO

if(MPI.COMM_WORLD.Get_rank() == 0):

    #Clear any existing status information
    with open(STAGE_DIR + "buildStatus", 'w') as f:
        f.write("CLEARING STATUS FROM PREVIOUS RUNS")

    #Clear temp folders from previous runs, if any
    shutil.rmtree(TMP_DIR)
    os.mkdir(TMP_DIR)
    
    statusFiles = glob.glob(STAT_DIR+"*")
    for f in statusFiles:
        os.remove(f)

    coreFiles = glob.glob(CORE_DIR+"*")
    for f in coreFiles:
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
    
    jobsPerNode = 10
    
    #IF VORTEX:
    #Running on 32GB machines with 12 cores each
    #nodeCount = comm_size/12
    
    #IF BORA:
    #Running on 128GB machines with 20 cores each.
    nodeCount = comm_size/20
    
    coresToAllocate = nodeCount * jobsPerNode
    
    #INITIALIZE CHECK FILES
    for core in range(1,(comm_size-1)):
        if(str(core)[-1:] in list(map(str, range(1,jobsPerNode)))):
            with open(CORE_DIR + str(core), 'w') as f:
                f.write("R")



    currentJob = 0
    maxJob = len(jobList)
    startTime = time.time()
    while True:
        errorCount = 0
        skipCount = 0
        STAT_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderWatch/"
        statusFiles = glob.glob(STAT_DIR+"*")

        for core in range(1,(comm_size-1)):
            if(str(core)[-1:] in list(map(str, range(1,jobsPerNode)))):
                with open(CORE_DIR + str(core), 'r') as f:
                    stat = f.read()
                if((stat == "R") and (currentJob <= (maxJob-1))):
                    chunk = [jobList[currentJob],core]
                    print("SENDING TO " + str(core) + ": " + str(chunk))
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
                else:
                    allOutcomes.append(v)
            
        percentDone = (allOutcomes.count("D")-skipCount) / (len(allOutcomes)-skipCount)*100

        if(allOutcomes.count("D") == len(allOutcomes)):
            with open(STAGE_DIR + "buildStatus", 'w') as f:
                f.write("BUILD IS COMPLETE.")
            sys.exit()
        else:
            with open(STAGE_DIR + "buildStatus", 'w') as f:
                f.write(str(round(percentDone,2)) + " percent complete (" + str(allOutcomes.count("D")-skipCount) + " of " + str(len(allOutcomes)-skipCount) + ", " + str(skipCount) + " skipped) | BUILD ERRORS: " + str(errorCount))
                    

else:


    while True:
        core = str(MPI.COMM_WORLD.Get_rank())
        with open(CORE_DIR + str(core), "w") as f:
            f.write("R")
        with open(CORE_LOGGING_DIR + str(core), "a") as f:
            f.write(str(time.ctime()) + ": I am waiting for a new job.")
        layers = comm.recv(source=0, tag=1)
        with open(CORE_LOGGING_DIR + str(core), "a") as f:
            f.write(str(time.ctime()) + ": I (" + str(MPI.COMM_WORLD.Get_rank()) + ", "+str(layers[1])+") have received " + str(layers[0]) + " layers to build.")
        with open(CORE_DIR + str(layers[1]), "w") as f:
            f.write(str(layers[0]))
        ret = []
        core = layers[1]
        l = layers[0]
        try:
            o = build(ISO=l[0], ADM=l[1], product=l[2])
            with open(CORE_LOGGING_DIR + str(core), "a") as f:
                f.write(str(time.ctime()) + ": I have completed the build assigned.\n")
        except:
            with open(CORE_LOGGING_DIR + str(core), "a") as f:
                f.write(str(time.ctime()) + ": I have encountered a build error.\n")
        ret.append(o)
        with open(STAT_DIR + "_" + l[0] + "_" + l[1] + "_" + l[2], 'w') as f:
            f.write(o[4])
        with open(CORE_DIR + str(core), "w") as f:
            f.write("D")
        time.sleep(15)
