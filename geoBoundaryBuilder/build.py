import pandas as pd
from dask.distributed import Client
from dask_jobqueue import PBSCluster
import os
import time

GB_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesDev/"
LOG_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/logs/"
TMP_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesTmp/"
#Limits total number of ADM units & resources requested.
TEST = True

f = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

cluster_kwargs = {
    "name": "globalSESCluster",
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:vortex:ppn=12",
    "walltime": "72:00:00",
    "cores": 12,
    "processes": 2,
    "memory": "32GB",
    "interface": "ib0",
}

cluster = PBSCluster(**cluster_kwargs)
if(TEST == True):
    cluster.scale(2)
else:
    cluster.scale(20)

admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
productTypes = ["geoBoundaries", "UN-SALB", "UN-OCHA"]
#Load in ISOs from master ISO list
countries = pd.read_csv("../dta/iso_3166_1_alpha_3.csv")
isoList = countries["Alpha-3code"].values

if(TEST == True):
    admTypes = ["ADM0"]
    productTypes = ["gbOpen"]
    isoList = ["USA", "MEX"]

jobList = {}
jobList["ADM"] = []
jobList["product"] = []
jobList["ISO"] = []
for adm in admTypes:
    for product in productTypes:
        for iso in isoList:
            jobList["ADM"].append(adm)
            jobList["product"].append(product)
            jobList["ISO"].append(iso)

print("Total Jobs: " + str(len(jobList["ADM"])))

def build(ISO, ADM, product):
    bnd = builder(ISO, ADM, product, GB_DIR, LOG_DIR, TMP_DIR)
    bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.time()))   
    
    validSource = bnd.checkSourceValidity()

    outcome=validSource
    return([ISO,ADM,product,outcome])

with Client(cluster) as client:
    futures = client.map(build, jobList["ISO"], jobList["ADM"], jobList["product"])
    result = [A.result() for A in futures]
    print(result)