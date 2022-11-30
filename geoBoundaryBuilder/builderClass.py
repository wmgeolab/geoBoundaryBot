import zipfile
import hashlib 
import os

class builder:
    def __init__(self, ISO, ADM, product, basePath, logPath, tmpPath):
        self.ISO = ISO
        self.ADM = ADM
        self.product = product
        self.basePath = basePath
        self.logPath = logPath
        self.tmpPath = tmpPath
        self.sourcePath = self.basePath + "sourceData/" + self.product + "/" + self.ISO + "_" + self.ADM + ".zip"

        #Checks
        self.existFail = 1
        self.metaFail = 1
        self.dataExtractFail = 1
    
    def logger(self, type, message):
        with open(self.logPath + str(self.ISO)+str(self.ADM)+str(self.product)+".log", "a") as f:
            f.write(str(type) + ": " + str(message) + "\n")
    
    def checkExistence(self):
        if os.path.exists(self.sourcePath):
            self.existFail = 0
            self.logger("File Exists: " + str(f))

    def metaLoad(self):
        try:
            with zipfile.ZipFile(self.sourcePath) as zF:
                self.meta = zF.read('meta.txt')
                self.metaFail = 0
        except Exception as e:
            self.logged("Metadata failed to load: " + str(e))

    def dataLoad(self):
        try:
            sourceZip = zipfile.ZipFile(self.sourcePath)
            sourceZip.extractall(self.tmpPath + + self.product + "/" + self.ISO + "_" + self.ADM + "/")
            if(os.path.exists(self.tmpPath + + self.product + "/" + self.ISO + "_" + self.ADM + "/" + "__MACOSX")):
                shutil.rmtree(self.tmpPath + + self.product + "/" + self.ISO + "_" + self.ADM + "/" + "__MACOSX")
            self.dataExtractFail = 0
        except Exception as e:
            self.logged("Zipfile extraction failed: " + str(e))


    def hashCalc(self):
        m = hashlib.sha256()
        chunkSize = 8192
        with open(self.basePath + "sourceData/" + self.product + "/" + self.ISO + "_" + self.ADM + ".zip", 'rb') as zF:
            while True:
                chunk = zF.read(chunkSize)
                if(len(chunk)):
                    m.update(chunk)
                else:
                    break
            #14 digit modulo on the hash of the zipfile.  Won't guarantee unique,
            #up from 8 based on gB 4.0 overlap concerns.
            self.metaHash = int(m.hexdigest(), 16) % 10**14
            self.logger("Hash Calculated: " + str(self.metaHash))
    
    def checkSourceValidity(self):
        descriptor = self.ISO + " | " + self.ADM + " | " + self.product
        self.checkExistence()
        if(self.existFail == 1):
            return(descriptor + ": Source file does not exist for this boundary.")
        
        self.metaLoad()
        if(self.metaFail == 1):
            return(descriptor + ": There is no meta.txt in the source zipfile for this boundary.")
        
        self.dataLoad()
        if(self.dataLoad == 1):
            return(descriptor + ": The zipfile in the source directory failed to extract correctly.")
        
        return(descriptor + ": source zipfile is valid.")