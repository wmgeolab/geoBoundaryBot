import os
import sys 

#This script prepares the *.gitattributes file for git lfs.
#We check if any files are >100MB, and if so tag them for LFS inclusion.

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries"
GA_PATH = GB_DIR + ".gitattributes"

#Clear old attributes file
try:
    os.remove(GA_PATH)
except:
    pass

for (path, dirname, filenames) in os.walk(GB_DIR):
    if(GB_DIR + "/.git/" not in path):
        for f in filenames:
            fPath = path + "/" + f
            if(os.path.getsize(fPath) > 100000000):
                print("File greater than 100MB Detected: " + str(fPath))
    
    