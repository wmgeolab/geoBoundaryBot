import pandas as pd
from dask.distributed import Client
from dask_jobqueue import PBSCluster
import os
import time

GB_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesDev/geoBoundaries/"
LOG_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/logs/"
TMP_DIR = "/sciclone/home20/dsmillerrunfol/geoBoundariesTmp/"
#Limits total number of ADM units & resources requested.
TEST = True

cluster_kwargs = {
    "name": "gbBuild-workers",
    "shebang": "#!/bin/tcsh",
    "resource_spec": "nodes=1:vortex:ppn=12",
    "walltime": "24:00:00",
    "cores": 12,
    "processes": 2,
    "memory": "32GB",
    "interface": "ib0",
}

cluster = PBSCluster(**cluster_kwargs)
if(TEST == True):
    cluster.scale(4)
else:
    cluster.scale(20)

f = "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/builderClass.py"
exec(compile(open(f, "rb").read(), f, 'exec'))

admTypes = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
productTypes = ["geoBoundaries", "UN_SALB", "UN_OCHA"]
#Load in ISOs from master ISO list
countries = pd.read_csv("../dta/iso_3166_1_alpha_3.csv")
isoList = countries["Alpha-3code"].values

licenses = pd.read_csv("../dta/gbLicenses.csv")
licenseList = licenses["license_name"].values

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

def build(ISO, ADM, product, validISO=isoList, validLicense=licenseList):
    bnd = builder(ISO, ADM, product, GB_DIR, LOG_DIR, TMP_DIR, validISO, licenseList)
    bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.time()))   
    
    validSource = bnd.checkSourceValidity()

    outcome=validSource
    return([ISO,ADM,product,outcome])

with Client(cluster) as client:
    futures = client.map(build, jobList["ISO"], jobList["ADM"], jobList["product"])
    result = [A.result() for A in futures]
    print(result)
    print("========")
    print("========")
    print("========")
    print("SHUTDOWN")
    print("========")
    print("========")
    print("========")