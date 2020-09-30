import os
import gbHelpers

check = gbHelpers.gbEnvVars("PASS", "", "r")
print(check)
if(check != "PASSED"):
    print("At least one stage of this check failed.")
    sys.exit("An artifact containing logs for this stage was generated.")