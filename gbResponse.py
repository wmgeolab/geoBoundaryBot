import gbHelpers
import os

checkType = "geometryDataCheck"
ws = os.environ['GITHUB_WORKSPACE']
sha = os.environ['gitsha']

in_path = os.path.expanduser("~") + "/tmp/www.logs.geoboundaries.org/artifacts/" + sha + "/"
theUrl = "http://www.logs.geoboundaries.org/artifacts/" + sha + "/"

checkTypes = ["fileChecks", "geometryDataChecks", "metaChecks"]

responseText = "**Hello!  I am the geoBoundary Bot**, and I just did an initial check on your submitted files. <br /><br />"

responseText = responseText + "I will automatically re-run my checks when you edit your Pull Request, and provide the new results in a comment just like this. <br />"

responseText = responseText + "Once all of your files meet my programmed standards, I will flag your file for a manual human review. <br />"


responseText = responseText + "<br /><br />=========== Submission Findings =========== <br /><br />"
#First, establish the overall finding, then report out the subfindings.
checkResults = {}
checkFailed = 0
for check in checkTypes:
    with open(in_path + check + "/RESULT.txt", "r") as f:
        checkResults[check] = f.read()
    
    if(checkResults[check] != "PASSED"):
        checkFailed = checkFailed + 1

if(checkFailed > 0):
    responseText = responseText + "**OVERALL STATUS**: " + str(checkFailed) + " checks are failing.  I have some recommendations for you on how you might fix these: <br />"

    for check in checkTypes:
        with open(in_path + check + "/RESULT.txt", "r") as f:
            checkResults[check] = f.read()
        
        if(checkResults[check] != "PASSED"):
            responseText = responseText + "**" + str(check) + "**: " + checkResults[check] + "  <br />"
            
        else:
            responseText = responseText + str(check) + ": PASSED.  Nothing that needs to be done here right now. <br />"
        
        responseText = responseText + "[Full logs for " + str(check) + "](" + theUrl + str(check) + "/" + str(check) +".txt" + ")  <br />"

    responseText = responseText + "I am going to attempt to visualize some of the data you sent.  Sometimes this fails if something is wrong with the data, otherwise you can see it here: <br />"
    responseText = responseText + "![Preview]("+ theUrl + "geometryDataChecks/preview.png)  <br />"

else:
    responseText = responseTest + "All checks have passed! I'll flag your boundary submission for a manual review by one of my humans.  <br />"
    responseText = responseText + "![Preview]("+ theUrl + "geometryDataChecks/preview.png)  <br />"

print(responseText)
with open(os.path.expanduser("~") + "/tmp/response.txt", "w") as f:
    f.write(responseText)