import os

#This script prepares the *.gitattributes file for git lfs.
#We check if any files are >100MB, and if so tag them for LFS inclusion.

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"
GA_PATH = GB_DIR + ".gitattributes"

#Clear old attributes file
try:
    os.remove(GA_PATH)
except:
    pass

for (path, dirname, filenames) in os.walk(ws["working"] + "/releaseData/"):
    print(datetime.now(), path)