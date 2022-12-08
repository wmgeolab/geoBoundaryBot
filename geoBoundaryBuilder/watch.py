#source "/usr/local/anaconda3-2021.05/etc/profile.d/conda.csh"
#module load anaconda3/2021.05
#conda activate geoBoundariesBuild

import pandas as pd
import glob
import time
import os

STAT_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesStat/"
statusFiles = glob.glob(STAT_DIR+"*")

statList = {}

while True:
    os.system('clear')

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

    headers = ["LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5","LAYER", "A0","A1","A2","A3","A4","A5"]
    count = 0
    
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
                print(bcolors.HEADER + '{0:<12}'.format(adm), end='')
            else:
                if(adm == "D"):
                    print(bcolors.OKGREEN + '{0:<3}'.format(adm), end='')
                elif(adm == "|"):
                    print(bcolors.OKBLUE + '{0:<3}'.format(adm), end='')
                elif(adm == "P"):
                    print(bcolors.WARNING + '{0:<3}'.format(adm), end='')
                else:
                    print(bcolors.FAIL + '{0:<3}'.format(adm), end='')
            count = count + 1
        if(rowCount == 0 or rowCount == 1 or rowCount == 2 or rowCount == 3):
            rowCount = rowCount + 1
        else:
            rowCount = 0
            print("\n")
    time.sleep(5)