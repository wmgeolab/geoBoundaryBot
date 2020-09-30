import os
import gbHelpers
import sys

check = gbHelpers.gbEnvVars("PASS", "", "r")
print(check)
if(check != "PASSED"):
    sys.exit("1")