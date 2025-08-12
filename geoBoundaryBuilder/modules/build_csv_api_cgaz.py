import os
import sys
import time
import psycopg2
import logging
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import re
import json
import shutil
import zipfile


#CSV Ouput file:
outputMetaCSV = "/sciclone/geograd/geoBoundaries/database/geoBoundariesDev/releaseData/geoBoundariesOpen-meta.csv"

#GB DIR:
GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundariesDev/"

url_static_base = "https://www.geoboundaries-dev.org/data/static/"
url_current_base = "https://www.geoboundaries-dev.org/data/current/"
apiPath = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/gbWeb/api/current/gbOpen/"

#For hashing
EXCLUDED_META_FIELDS = ("buildDate", "sourceDataUpdateDate")

# Logging setup
log_dir = "/sciclone/geograd/geoBoundaries/logs/final_build_worker/"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "final_build_worker.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)]
)

# Database config (edit if needed)
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""
DB_PORT = 5432

def fast_copy(src, dst, buffer_size=16 * 1024 * 1024):  # 16 MB buffer
    total_size = os.path.getsize(src)
    copied = 0
    bar_length = 40
    with open(src, 'rb') as fsrc, open(dst, 'wb') as fdst:
        while True:
            buf = fsrc.read(buffer_size)
            if not buf:
                break
            fdst.write(buf)
            copied += len(buf)
            percent = copied / total_size
            filled_len = int(bar_length * percent)
            bar = '=' * filled_len + '-' * (bar_length - filled_len)
            sys.stdout.write(f'\rCopying: |{bar}| {percent:.0%} ({copied // (1024*1024)}MB/{total_size // (1024*1024)}MB)')
            sys.stdout.flush()
        sys.stdout.write('\n')


def connect_to_db():
    return psycopg2.connect(
        host=DB_SERVICE,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def get_full_db_build_status():
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT "STATUS" FROM status WHERE "STATUS_TYPE" = %s', ("FULL_DB_BUILD",))
                row = cur.fetchone()
                status_val = row[0] if row else None
                return status_val
    except Exception as e:
        logging.error(f"Error fetching FULL_DB_BUILD status: {e}")
        return None

def get_ready_task_count():
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT COUNT(*) FROM tasks WHERE status = %s', ("ready",))
                count = cur.fetchone()[0]
                return count
    except Exception as e:
        logging.error(f"Error counting ready tasks: {e}")
        return None

def main():
    # On first run, check and log FULL_DB_BUILD status
    get_full_db_build_status()
    
    while True:
        ready_count = get_ready_task_count()
        if ready_count is None:
            logging.error("Could not retrieve ready task count. Retrying in 10 minutes.")
            time.sleep(600)
            continue
        logging.info(f"Ready tasks count: {ready_count}")
        if ready_count == 0:
            logging.info("No ready tasks. Beginning processing.")
            print("No ready tasks. Beginning processing.")
            logging.info(get_full_db_build_status())

            logging.info("Beginning construction of metadata CSV files and API.")
            
            # Delete old CSV
            try:
                os.remove(outputMetaCSV)
                logging.info(f"Deleted old CSV at {outputMetaCSV}")
            except FileNotFoundError:
                logging.info(f"No old CSV to delete at {outputMetaCSV}")
            except Exception as e:
                logging.warning(f"Could not delete old CSV: {e}")

            # Create boundary_meta table if not exists
            try:
                with connect_to_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                        CREATE TABLE IF NOT EXISTS boundary_meta (
                          boundaryISO             text NOT NULL,
                          boundaryType            text NOT NULL,
                          boundaryID              text,
                          boundaryYear            integer,
                          boundarySource          text,
                          boundaryCanonical       text,
                          boundaryLicense         text,
                          licenseDetail           text,
                          licenseSource           text,
                          boundarySourceURL       text,
                          sourceDataUpdateDate    timestamptz,
                          buildDate               timestamptz,
                          admUnitCount            integer,
                          meanVertices            numeric,
                          minVertices             integer,
                          maxVertices             integer,
                          meanPerimeterLengthKM   numeric,
                          maxPerimeterLengthKM    numeric,
                          minPerimeterLengthKM    numeric,
                          meanAreaSqKM            numeric,
                          minAreaSqKM             numeric,
                          maxAreaSqKM             numeric,
                          PRIMARY KEY (boundaryISO, boundaryType)
                        );
                        """)
                        conn.commit()
                logging.info("Ensured boundary_meta table exists.")
            except Exception as e:
                logging.error(f"Failed to create boundary_meta table: {e}")
                raise
        
            # Load in ISO codes
            try:
                isoDetails = pd.read_csv("../../dta/iso_3166_1_alpha_3.csv", encoding='utf-8')
                logging.info("Loaded ISO code details.")
            except Exception as e:
                logging.error(f"Failed to load ISO codes: {e}")
                raise
            jsonDict = {}
            for (path, dirname, filenames) in os.walk(GB_DIR + "releaseData/gbOpen"):
                logging.info(f"Entering directory: {path}")
                metaSearch = [x for x in filenames if re.search('metaData.json', x)]
                if len(metaSearch) == 1 and "ADM" in path:
                    json_path = path + "/" + metaSearch[0]
                    logging.info(f"Found metaData.json: {json_path}")
                    try:
                        with open(json_path, encoding='utf-8', mode="r") as j:
                            meta = json.load(j)
                        logging.info(f"Loaded JSON metadata for {json_path}")
                    except Exception as e:
                        logging.error(f"Failed to load JSON {json_path}: {e}")
                        continue
                    
                    isoMeta = isoDetails[isoDetails["Alpha-3code"] == meta['boundaryISO']]
                    logging.info(f"Found ISO metadata for {meta['boundaryISO']}")
                    # Build the metadata
                    metaLine = '"' + meta['boundaryID'] + '","' + isoMeta["Name"].values[0] + '","' + meta['boundaryISO'] + '","' + meta['boundaryYear'] + '","' + meta["boundaryType"] + '","'

                    if("boundaryCanonical" in meta):
                        if(len(meta["boundaryCanonical"])>0):
                            bndCan = meta["boundaryCanonical"]
                            metaLine = metaLine + meta["boundaryCanonical"] + '","'
                        else:
                            metaLine = metaLine + 'Unknown","'
                            bndCan = "Unknown"
                    else:
                        metaLine = metaLine + 'Unknown","'

                    # Cleanup free-form text fields
                    meta['licenseDetail'] = meta["licenseDetail"].replace(',','')
                    meta['licenseDetail'] = meta["licenseDetail"].replace('\\','')
                    meta['licenseDetail'] = meta["licenseDetail"].replace('"','')

                    metaLine = metaLine + meta['boundarySource'] + '","' + meta['boundaryLicense'] + '","' + meta['licenseDetail'].replace("https//","").replace("https://","").replace("http//","").replace("http://","") + '","' + meta['licenseSource'].replace("https//","").replace("https://","").replace("http//","").replace("http://","")  + '","'
                    metaLine = metaLine + meta['boundarySourceURL'].replace("https//","https://").replace("https://","").replace("http//","").replace("http://","")  + '","' + meta['sourceDataUpdateDate'] + '","' + meta["buildDate"] + '","'
                    
                    metaLine = metaLine + isoMeta["Continent"].values[0] + '","' + isoMeta["UNSDG-region"].values[0] + '","'
                    metaLine = metaLine + isoMeta["UNSDG-subregion"].values[0] + '","' 
                    metaLine = metaLine + isoMeta["worldBankIncomeGroup"].values[0] + '","'

                    # Append geometry stats
                    metaLine = metaLine + str(meta["admUnitCount"]) + '","' + str(meta["meanVertices"]) + '","' + str(meta["minVertices"]) + '","' + str(meta["maxVertices"]) + '","'
                    metaLine = metaLine + str(meta["meanPerimeterLengthKM"]) + '","' + str(meta["minPerimeterLengthKM"]) + '","' + str(meta["maxPerimeterLengthKM"]) + '","'
                    metaLine = metaLine + str(meta["meanAreaSqKM"]) + '","' + str(meta["minAreaSqKM"]) + '","' + str(meta["maxAreaSqKM"]) + '","'
                    
                    # Cleanup
                    metaLine = metaLine.replace("nan","")

                    # Add static link and copy file over appropriately
                    def hash_meta(meta: dict) -> str:
                        meta_copy = {k: v for k, v in meta.items() if k not in EXCLUDED_META_FIELDS}
                        meta_str = json.dumps(meta_copy, sort_keys=True)
                        return hashlib.sha256(meta_str.encode('utf-8')).hexdigest()

                    def hash_meta_from_zip(zip_path: str, iso: str, level: str) -> str:
                        target_file_name = f"geoBoundaries-{iso}-{level}-metaData.json"
                        with zipfile.ZipFile(zip_path, 'r') as z:
                            with z.open(target_file_name) as f:
                                zip_meta = json.load(f)
                        return hash_meta(zip_meta)


                    static_file_path = path + "/" + "geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + "-all.zip"
                    unique_static_link = hash_meta(meta)
                    static_link_file = f"geoBoundaries-{meta['boundaryISO']}-{meta['boundaryType']}-all-{unique_static_link}.zip"



                    dest_path = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/gbWeb/data/static/" + static_link_file
                    need_copy = True
                    if os.path.exists(dest_path):
                        # Compare hashes to confirm if files are the same
                        dest_hash = hash_meta_from_zip(static_file_path, meta["boundaryISO"], meta["boundaryType"])
                        if dest_hash == unique_static_link:
                            logging.info(f"Static file already exists and is identical: {dest_path}. Skipping copy.")
                            need_copy = False
                        else:
                            logging.info(f"Static file exists but is different. Overwriting: {dest_path}")
                    if need_copy:
                        try:
                            logging.info(f"Copying static file {static_file_path} to {dest_path}")
                            fast_copy(static_file_path, dest_path)
                            logging.info(f"Copied static file to web server: {static_link_file}")
                        except Exception as e:
                            logging.error(f"Failed to copy static file {static_file_path}: {e}")
                            continue

                    metaLine = metaLine + url_static_base + static_link_file 
                    # Newline
                    metaLine = metaLine + '"\n'

                    # Insert or update metadata row in boundary_meta
                    try:
                        with connect_to_db() as conn:
                            with conn.cursor() as cur:
                                cur.execute("""
                                    INSERT INTO boundary_meta (
                                        boundaryISO, boundaryType, boundaryID, boundaryYear, boundarySource, boundaryCanonical, boundaryLicense, licenseDetail, licenseSource, boundarySourceURL, sourceDataUpdateDate, buildDate, admUnitCount, meanVertices, minVertices, maxVertices, meanPerimeterLengthKM, maxPerimeterLengthKM, minPerimeterLengthKM, meanAreaSqKM, minAreaSqKM, maxAreaSqKM
                                    ) VALUES (
                                        %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
                                    )
                                    ON CONFLICT (boundaryISO, boundaryType) DO UPDATE SET
                                        boundaryID=EXCLUDED.boundaryID,
                                        boundaryYear=EXCLUDED.boundaryYear,
                                        boundarySource=EXCLUDED.boundarySource,
                                        boundaryCanonical=EXCLUDED.boundaryCanonical,
                                        boundaryLicense=EXCLUDED.boundaryLicense,
                                        licenseDetail=EXCLUDED.licenseDetail,
                                        licenseSource=EXCLUDED.licenseSource,
                                        boundarySourceURL=EXCLUDED.boundarySourceURL,
                                        sourceDataUpdateDate=EXCLUDED.sourceDataUpdateDate,
                                        buildDate=EXCLUDED.buildDate,
                                        admUnitCount=EXCLUDED.admUnitCount,
                                        meanVertices=EXCLUDED.meanVertices,
                                        minVertices=EXCLUDED.minVertices,
                                        maxVertices=EXCLUDED.maxVertices,
                                        meanPerimeterLengthKM=EXCLUDED.meanPerimeterLengthKM,
                                        maxPerimeterLengthKM=EXCLUDED.maxPerimeterLengthKM,
                                        minPerimeterLengthKM=EXCLUDED.minPerimeterLengthKM,
                                        meanAreaSqKM=EXCLUDED.meanAreaSqKM,
                                        minAreaSqKM=EXCLUDED.minAreaSqKM,
                                        maxAreaSqKM=EXCLUDED.maxAreaSqKM;
                                """,
                                (
                                    meta['boundaryISO'],
                                    meta['boundaryType'],
                                    meta['boundaryID'],
                                    int(meta['boundaryYear']) if meta['boundaryYear'] else None,
                                    meta['boundarySource'],
                                    bndCan,
                                    meta['boundaryLicense'],
                                    meta['licenseDetail'],
                                    meta['licenseSource'],
                                    meta['boundarySourceURL'],
                                    meta['sourceDataUpdateDate'],
                                    meta['buildDate'],
                                    int(meta['admUnitCount']) if meta['admUnitCount'] else None,
                                    float(meta['meanVertices']) if meta['meanVertices'] else None,
                                    int(meta['minVertices']) if meta['minVertices'] else None,
                                    int(meta['maxVertices']) if meta['maxVertices'] else None,
                                    float(meta['meanPerimeterLengthKM']) if meta['meanPerimeterLengthKM'] else None,
                                    float(meta['maxPerimeterLengthKM']) if meta['maxPerimeterLengthKM'] else None,
                                    float(meta['minPerimeterLengthKM']) if meta['minPerimeterLengthKM'] else None,
                                    float(meta['meanAreaSqKM']) if meta['meanAreaSqKM'] else None,
                                    float(meta['minAreaSqKM']) if meta['minAreaSqKM'] else None,
                                    float(meta['maxAreaSqKM']) if meta['maxAreaSqKM'] else None
                                )
                                )
                                conn.commit()
                        logging.info(f"Upserted metadata for {meta['boundaryISO']}-{meta['boundaryType']}")
                    except Exception as e:
                        logging.error(f"Failed to upsert meta for {meta['boundaryISO']}-{meta['boundaryType']}: {e}")

                    # Write the new API JSON dynamically using meta and isoMeta values

                    if meta['boundaryISO'] not in jsonDict:
                        jsonDict[meta['boundaryISO']] = []
                        

                    api_json = {
                        "boundaryID": meta['boundaryID'],
                        "boundaryName": isoMeta["Name"].values[0],
                        "boundaryISO": meta['boundaryISO'],
                        "boundaryYearRepresented": meta['boundaryYear'],
                        "boundaryType": meta['boundaryType'],
                        "boundaryCanonical": bndCan,
                        "boundarySource": meta['boundarySource'],
                        "boundaryLicense": meta['boundaryLicense'],
                        "licenseDetail": meta["licenseDetail"],
                        "licenseSource": meta["licenseSource"],
                        "boundarySourceURL": meta['boundarySourceURL'],
                        "sourceDataUpdateDate": meta['sourceDataUpdateDate'],
                        "buildDate": meta["buildDate"],
                        "Continent": isoMeta["Continent"].values[0],
                        "UNSDG-region": isoMeta["UNSDG-region"].values[0],
                        "UNSDG-subregion": isoMeta["UNSDG-subregion"].values[0],
                        "worldBankIncomeGroup": isoMeta["worldBankIncomeGroup"].values[0],
                        "admUnitCount": str(meta["admUnitCount"]),
                        "meanVertices": str(meta["meanVertices"]),
                        "minVertices": str(meta["minVertices"]),
                        "maxVertices": str(meta["maxVertices"]),
                        "meanPerimeterLengthKM": str(meta["meanPerimeterLengthKM"]),
                        "minPerimeterLengthKM": str(meta["minPerimeterLengthKM"]),
                        "maxPerimeterLengthKM": str(meta["maxPerimeterLengthKM"]),
                        "meanAreaSqKM": str(meta["meanAreaSqKM"]),
                        "minAreaSqKM": str(meta["minAreaSqKM"]),
                        "maxAreaSqKM": str(meta["maxAreaSqKM"]),
                        "staticDownloadLink": url_static_base + static_link_file,
                        "gjDownloadURL": url_current_base + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + ".geojson",
                        "tjDownloadURL": url_current_base + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + ".topojson",
                        "imagePreview": url_current_base + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + "-PREVIEW.png",
                        "simplifiedGeometryGeoJSON": url_current_base + meta['boundaryISO'] + "/" + meta["boundaryType"] + "/geoBoundaries-" + meta['boundaryISO'] + "-" + meta["boundaryType"] + "_simplified.geojson",
                    }

                    api_dir = os.path.join(apiPath, meta['boundaryISO'], meta['boundaryType'])
                    os.makedirs(api_dir, exist_ok=True)
                    with open(os.path.join(api_dir, "index.json"), "w", encoding="utf-8") as jf:
                        json.dump(api_json, jf, ensure_ascii=False, indent=2)

                    jsonDict[meta['boundaryISO']].append(api_json)

                elif "ADM" not in path:
                    pass

                else:
                    error_msg = "Error building CSV: More than one metadata search return for {path}"
                    print("ERROR: " + str(path) + " " + str(metaSearch))
                    try:
                        with connect_to_db() as conn:
                            with conn.cursor() as cur:
                                cur.execute('UPDATE status SET "STATUS" = %s, "TIME" = NOW() WHERE "STATUS_TYPE" = %s', (error_msg, "FULL_DB_BUILD"))
                                conn.commit()
                    except Exception as db_exc:
                        logging.error(f"Failed to update status table with error: {db_exc}")

            # After all upserts, export full table to outputMetaCSV
            try:
                with connect_to_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT * FROM boundary_meta ORDER BY boundaryISO, boundaryType;")
                        rows = cur.fetchall()
                        colnames = [desc[0] for desc in cur.description]
                # Write CSV
                with open(outputMetaCSV, 'w', encoding='utf-8') as f:
                    # Write header (add Continent, UNSDG-region, etc. by joining isoDetails)
                    f.write("boundaryID,boundaryName,boundaryISO,boundaryYearRepresented,boundaryType,boundaryCanonical,boundarySource,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,staticDownloadLink\n")
                    for row in rows:
                        # Lookup ISO meta
                        iso = row[colnames.index('boundaryISO')]
                        iso_row = isoDetails[isoDetails["Alpha-3code"] == iso]
                        # Compose CSV line
                        f.write(",".join([
                            f'"{row[colnames.index("boundaryID")]}"',
                            f'"{iso_row["Name"].values[0] if not iso_row.empty else ""}"',
                            f'"{row[colnames.index("boundaryISO")]}"',
                            f'"{row[colnames.index("boundaryYear")] if row[colnames.index("boundaryYear")] is not None else ""}"',
                            f'"{row[colnames.index("boundaryType")]}"',
                            f'"{row[colnames.index("boundaryCanonical")]}"',
                            f'"{row[colnames.index("boundarySource")]}"',
                            f'"{row[colnames.index("boundaryLicense")]}"',
                            f'"{row[colnames.index("licenseDetail")]}"',
                            f'"{row[colnames.index("licenseSource")]}"',
                            f'"{row[colnames.index("boundarySourceURL")]}"',
                            f'"{row[colnames.index("sourceDataUpdateDate")]}"',
                            f'"{row[colnames.index("buildDate")]}"',
                            f'"{iso_row["Continent"].values[0] if not iso_row.empty else ""}"',
                            f'"{iso_row["UNSDG-region"].values[0] if not iso_row.empty else ""}"',
                            f'"{iso_row["UNSDG-subregion"].values[0] if not iso_row.empty else ""}"',
                            f'"{iso_row["worldBankIncomeGroup"].values[0] if not iso_row.empty else ""}"',
                            f'"{row[colnames.index("admUnitCount")]}"',
                            f'"{row[colnames.index("meanVertices")]}"',
                            f'"{row[colnames.index("minVertices")]}"',
                            f'"{row[colnames.index("maxVertices")]}"',
                            f'"{row[colnames.index("meanPerimeterLengthKM")]}"',
                            f'"{row[colnames.index("minPerimeterLengthKM")]}"',
                            f'"{row[colnames.index("maxPerimeterLengthKM")]}"',
                            f'"{row[colnames.index("meanAreaSqKM")]}"',
                            f'"{row[colnames.index("minAreaSqKM")]}"',
                            f'"{row[colnames.index("maxAreaSqKM")]}"',
                            # staticDownloadLink cannot be reconstructed here; leave blank or reconstruct if hash logic is available
                            '""'
                        ]) + "\n")
                logging.info(f"Exported metadata table to {outputMetaCSV}")
            except Exception as e:
                logging.error(f"Failed to export metadata table to CSV: {e}")

            #Build the "ALL" cases for each ISO in the API
            for iso, adm_entries in jsonDict.items():
                out_dir = os.path.join(apiPath, iso, "ALL")
                os.makedirs(out_dir, exist_ok=True)

                out_path = os.path.join(out_dir, "index.json")

                with open(out_path, "w", encoding="utf-8") as jf:
                    json.dump(adm_entries, jf, ensure_ascii=False, indent=2)
            
            #ALL/ALL - every boundary in the world
            all_jsons = []
            for iso_entries in jsonDict.values():
                all_jsons.extend(iso_entries)

            global_all_dir = os.path.join(apiPath, "ALL")
            global_all_all = os.path.join(global_all_dir, "ALL")
            os.makedirs(global_all_dir, exist_ok=True)
            os.makedirs(global_all_all, exist_ok=True)

            global_all_path = os.path.join(global_all_all, "index.json")

            with open(global_all_path, "w", encoding="utf-8") as f:
                json.dump(all_jsons, f, ensure_ascii=False, indent=2)

            #ALL/ADMX - every boundary in the world at a specific ADMX
            adm_levels = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]

            for adm in adm_levels:
                # Pick only the objects whose boundaryType matches this ADM level
                adm_entries = [j for j in all_jsons if j["boundaryType"] == adm]


                # Create …/ALL/ADM#/index.json
                out_dir = os.path.join(apiPath, "ALL", adm)
                os.makedirs(out_dir, exist_ok=True)

                out_path = os.path.join(out_dir, "index.json")
                with open(out_path, "w", encoding="utf-8") as f:
                    json.dump(adm_entries, f, ensure_ascii=False, indent=2)

            # Update DB status to indicate CSV construction is complete
            success_msg = "CSV and API Construction complete"
            try:
                with connect_to_db() as conn:
                    with conn.cursor() as cur:
                        cur.execute('UPDATE status SET "STATUS" = %s, "TIME" = NOW() WHERE "STATUS_TYPE" = %s', (success_msg, "FULL_DB_BUILD"))
                        conn.commit()
                logging.info(f"Updated status table: {success_msg}")
            except Exception as db_exc:
                logging.error(f"Failed to update status table with success message: {db_exc}")

            #

                

            logging.info("Processing placeholder block executed.")
            # ----------------------------------------
            logging.info("Processing complete. Sleeping for 72 hours.")
            time.sleep(72 * 3600)  # 72 hours
        else:
            logging.info(f"{ready_count} tasks still ready. Sleeping for 1 hour.")
            time.sleep(1800)  # 30 minutes

if __name__ == "__main__":
    main()
