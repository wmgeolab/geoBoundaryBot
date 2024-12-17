import sys
import os
import zipfile

# Extract ISO and ADM parameters
iso = sys.argv[1]
adm = sys.argv[2]

# Directory configuration
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"

# File to process
filename = f"{iso}_{adm}.zip"
file_path = os.path.join(TASK_DIR, filename)

# Process the file
print(f"Processing file: {file_path}")

try:
    with zipfile.ZipFile(file_path, 'r') as zip_ref:
        file_list = zip_ref.namelist()
        print(f"Contents of {filename}:")
        for file in file_list:
            print(f" - {file}")
except Exception as e:
    print(f"Failed to process file {filename}: {e}")
