import subprocess

GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"

#Get list of all commits between the current branch and the main branch
gitCommitLog = "cd " + GB_DIR + "; git rev-list --reverse origin/main..HEAD"
log = subprocess.check_output(gitCommitLog, shell=True)
commits = log.decode('UTF-8').split("\n")

print("Commits to push: " + str(len(commits)))

counter = 0
for commit in commits:
    print("Pushing commit " + str(counter) + " of " + str(len(commits)) + " (" + commit + ")")
    if(len(commit) > 10):
        gitPush = "cd " + GB_DIR + "; git push -f origin " + commit + ":main"
        try:
            gitPushOutcome = subprocess.check_output(gitPush, shell=True) 
            print(gitPushOutcome)
        except Exception as e:
            print("ERROR: " + str(e))
        counter = counter + 1