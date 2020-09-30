import os

if(os.environ["PASS"] != "PASSED"):
    print("At least one stage of this check failed.")
    sys.exit("An artifact containing logs for this stage was generated.")