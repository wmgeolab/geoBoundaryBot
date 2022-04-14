import gbHelpers
import sys

check = gbHelpers.gbEnvVars("RESULT", "", "r")
print(check)
if(check != "PASSED"):
    sys.exit("1")
