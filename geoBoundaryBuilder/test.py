import pandas as pd
from dask.distributed import Client
from dask_jobqueue import PBSCluster
import os
import time

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
cluster.scale(2)

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
    bnd = builder(ISO, ADM, product, "/sciclone/home20/dsmillerrunfol/geoBoundariesDev/", "/sciclone/home20/dsmillerrunfol/geoBoundaryBot/geoBoundaryBuilder/logs/", "/sciclone/home20/dsmillerrunfol/geoBoundariesTmp/")
    bnd.logger("\n\n\nLAYER BUILD COMMENCE TIMESTAMP", str(time.time()))   
    
    validSource = bnd.checkSourceValidity()

    outcome=validSource
    return([ISO,ADM,product,outcome])

with Client(cluster) as client:
    futures = client.map(build, jobList["ISO"], jobList["ADM"], jobList["product"])
    result = [A.result() for A in futures]
    print(result)