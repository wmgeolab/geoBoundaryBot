
import os
import csv

def initiateWorkspace(check):

    ws = {}
    try:
        ws['working'] = os.environ['GITHUB_WORKSPACE']
        ws['changedFiles'] = os.environ['changes'].strip('][').split(',')
        ws['logPath'] = os.path.expanduser("~") + "/tmp/" + str(check) + ".txt"
        ws['zips'] = list(filter(lambda x: x[-4:] == '.zip', ws["changedFiles"]))
    except:
        ws['working'] = "/home/dan/git/gbRelease"
        ws['changedFiles'] = ['sourceData/gbOpen/ARE_ADM1.zip', 'sourceData/gbOpen/QAT_ADM0.zip', 'sourceData/gbOpen/FRA_ADM5.zip']
        ws['logPath'] = ws["working"] + "/tmp/sha/" + str(check) + ".txt"
        ws['zips'] = list(filter(lambda x: x[-4:] == '.zip', ws["changedFiles"]))

    print("Python WD: " + ws['working'])  
    print("Python changedFiles: " + str(ws['changedFiles']))
    print("Logging Path: " + str(ws["logPath"]))
    print("Changed Zips Detected: " + str(ws['zips']))

    
    return ws

def logWrite(check, line):
    print(line)
    with open(os.path.expanduser("~") + "/tmp/" + str(check) + ".txt", "a") as f:
        f.write(line + "\n")

def checkRetrieveLFSFiles(z, workingDir="./"):
    with open(workingDir + "/.gitattributes") as f:
        lfsList = list(csv.reader(f, delimiter=" "))
    
    lfsFiles = [i[0] for i in lfsList]
    if(z in lfsFiles):
        print("")
        print("--------------------------------")
        print("Downloading LFS File (file > 25mb): " + z)
        os.system('git lfs pull --include=\"' + z +'\"')
        
    else:
        print("")
        print("--------------------------------")
        print("No download from LFS required (file < 25mb): " + z)
        print("")
        return(0)

def gbEnvVars(varName, content,mode):
    if(mode == "w"):
        with open(os.path.expanduser(~) + "/tmp/" + varname + ".txt", w+):
            f.write(content)
    if(mode == "r"):
        with open(os.path.expanduser(~) + "/tmp/" + varname + ".txt", r):
            return f.read()
