#source "/usr/local/anaconda3-2021.05/etc/profile.d/conda.csh"
#module load anaconda3/2021.05
#conda activate geoBoundariesBuild

import pandas as pd
import glob
import time
import os
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKCYAN = '\033[96m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

while True:
    os.system('clear')
    STAT_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderWatch/"
    STAGE_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderStage/"
    CORE_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilderCore/"
    
    try: 
        with open(STAGE_DIR + "buildStatus", 'r') as f:
            bStat = f.read()
    except:
        bStat = "No Ongoing Build"

    if(bStat == "CLEARING STATUS FROM PREVIOUS RUNS" or bStat == "BUILD HAS COMMENCED."):
        print()
        print(bcolors.HEADER +'{0:<15}'.format("BUILD STATUS: "), end='')
        print(bcolors.WARNING +'{0:<30}'.format(bStat), end='')
        print()
    else:

        statusFiles = glob.glob(STAT_DIR+"*")
        statList = {}
        for f in statusFiles:
            with open(f,"r") as fHandler:
                v = fHandler.read()
                iso = str(f).split("_")[1]
                adm = str(f).split("_")[2]
                product = str(f).split("_")[3]
                
                key = iso + "_" + product

                if(key not in statList.keys()):
                    statList[key] = ["", "|","|","|","|","|","|"]
                
                if(adm == "ADM0"):
                    statList[key][1] = v
                if(adm == "ADM1"):
                    statList[key][2] = v
                if(adm == "ADM2"):
                    statList[key][3] = v
                if(adm == "ADM3"):
                    statList[key][4] = v
                if(adm == "ADM4"):
                    statList[key][5] = v
                if(adm == "ADM5"):
                    statList[key][6] = v
                
                statList[key][0] = key



        headers = ["LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5"]
        count = 0
        print()
        for h in headers:
            if (count == 0 or count == 7 or count == 14 or count == 21 or count == 28):
                print('{0:<12}'.format(h), end='')
            else:
                print('{0:<3}'.format(h), end='')
            count = count + 1
            
        print()
        rowCount = 0
        for dta in statList:
            count = 0
            for adm in statList[dta]:
                if (count == 0 or count == 7 or count == 14 or count == 21 or count == 28):
                    print(bcolors.HEADER + '{0:<12}'.format(adm[0:10]), end='')
                else:
                    if(adm == "D"):
                        print(bcolors.OKGREEN + '{0:<3}'.format(adm), end='')
                    elif(adm == "|"):
                        print(bcolors.OKBLUE + '{0:<3}'.format(adm), end='')
                    elif(adm == "P"):
                        print(bcolors.OKBLUE + '{0:<3}'.format(adm), end='')
                    elif(adm == "S"):
                        print(bcolors.OKCYAN + '{0:<3}'.format(adm), end='')
                    elif(adm == "L" or adm == "V" or adm == "M" or adm=="M" or adm=="G"):
                        print(bcolors.WARNING + '{0:<3}'.format(adm), end='')
                    else:
                        print(bcolors.FAIL + '{0:<3}'.format(adm), end='')
                count = count + 1
            if(rowCount == 0 or rowCount == 1 or rowCount == 2 or rowCount == 3):
                rowCount = rowCount + 1
            else:
                rowCount = 0
                print()
        print()

        for h in headers:
            if (count == 0 or count == 7 or count == 14 or count == 21 or count == 28):
                print(bcolors.HEADER + '{0:<12}'.format(h), end='')
            else:
                print(bcolors.HEADER + '{0:<3}'.format(h), end='')
            count = count + 1 
        
        print()
        print()
        #Check the precent of a build, if one is ongoing.
        print(bcolors.HEADER +'{0:<20}'.format(str(time.ctime() + ":")), end='')
        if(bStat == "No Ongoing Build"):
            print(bcolors.WARNING +'{0:<100}'.format(bStat), end='')
        elif("CRITICAL" in bStat):
            print(bcolors.FAIL +'{0:<100}'.format(bStat), end='')
        elif("COMPLETE" in bStat):
            print(bcolors.OKGREEN +'{0:<100}'.format(bStat), end='')
        else:
            print(bcolors.OKBLUE +'{0:<100}'.format(bStat), end='')
        print()
        print()
        #Parse CPU activity.
        coreFiles = glob.glob(CORE_DIR+"*")
        print(bcolors.HEADER + "Core Activity:")
        coreCounter = 0
        for f in coreFiles:
            with open(f,"r") as cHandler:
                v = cHandler.read()
            core = str(f).split("/")[-1]
            if(coreCounter < 3):
                print(bcolors.ENDC + '{0:50}'.format(core + "-" + str(v)), end='')
                coreCounter = coreCounter + 1
            else:
                coreCounter = 0
                print()
                print(bcolors.ENDC + '{0:50}'.format(core + "-" + str(v)), end='')
        
        print()
        print()

        
    time.sleep(15)