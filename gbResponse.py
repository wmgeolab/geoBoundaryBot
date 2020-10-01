import gbHelpers.py
import os

checkType = "geometryDataCheck"
ws = os.environ['GITHUB_WORKSPACE']
sha = os.environ['gitsha']

in_path = os.path.expanduser("~") + "/tmp/www.logs.geoboundaries.org/artifacts/" + sha + "/"
theUrl = "http://www.logs.geoboundaries.org/artifacts/" + sha + "/"

checkTypes = ["fileChecks", "geometryDataChecks", "metaChecks"]

responseText = "Hello!  I am the geoBoundary Bot, and I just did an initial check on your submitted files. \\\\"

responseText = responseText + "If you have submitted something other than a new boundary (i.e., new code), then you can safely disregard me. \\\\"

responseText = responseText + "I will automatically re-run my checks when you edit your Pull Request, and provide the new results in a comment just like this. \\\\"

responseText = responseText + "Once all of your files meet my programmed standards, I will flag your file for a manual review. \\\\"
responseText = responseText + "  \\\\"


responseText = responseText + "=========== Submission Findings ==========="
#First, establish the overall finding, then report out the subfindings.
checkResults = {}
checkFailed = 0
for check in checkTypes:
    with open(in_path + check + "/RESULT.txt", "r") as f:
        checkResults[check] = f.read()
    
    if(checkResults[check] != "PASSED"):
        checkFailed = checkFailed + 1

if(checkFailed > 0):
    responseText = responseText + "OVERALL STATUS: " + str(checkFailed) + " checks are failing.  I have some recommendations for you on how you might fix these: \\\\"
    responseText = responseText + "  \\\\"

    for check in checkTypes:
        with open(in_path + check + "/RESULT.txt", "r") as f:
            checkResults[check] = f.read()
        
        if(checkResults[check] != "PASSED"):
            responseText = responseText + str(check) + ": " + checkResults[check] + "  \\\\"
            
        else:
            responseText = responseText + str(check) + ": PASSED.  Nothing that needs to be done here right now. \\\\"
        
        responseText = responseText + "Full logs for " str(check) + ": " + theUrl + str(check) + "\\\\" + str(check) +".txt" + "  \\\\"

    responseText = responseText + "I am going to attempt to visualize a map for you.  Sometimes this fails if something is wrong with the data, otherwise you can see it here: \\\\"
    responseText = responseText + "![Preview]("+ theUrl + "geometryDataChecks\preview.png)  \\\\"

else:
    responseText = responseTest + "All checks have passed! I'll flag your boundary submission for a manual review by one of my humans.  \\\\"
    responseText = responseText + "![Preview]("+ theUrl + "geometryDataChecks\preview.png)  \\\\"


os.environ["RESPONSE"] = responseText
os.system("export RESPONSE='" + responseText + "'")