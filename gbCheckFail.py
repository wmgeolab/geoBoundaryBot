import sys

import gbHelpers

check = gbHelpers.gbEnvVars("RESULT", "", "r")
print(check)
if check != "PASSED":
    sys.exit("1")
