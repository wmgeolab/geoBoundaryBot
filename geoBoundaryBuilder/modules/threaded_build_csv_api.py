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
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict

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
import threading
log_dir = "/sciclone/geograd/geoBoundaries/logs/final_build_worker/"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "final_build_worker.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)]
)
# Global lock for logging
log_lock = threading.Lock()

# Database config (edit if needed)
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""
DB_PORT = 5432


def fast_copy(src, dst, buffer_size=16 * 1024 * 1024):  # 16 MB buffer
    import os
    import sys
    file_name = os.path.basename(src)
    total_size = os.path.getsize(src)
    copied = 0
    bar_length = 40
    print(f"Copying file: {file_name}")
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
            sys.stdout.write(f'\r{file_name}: |{bar}| {percent:.0%} ({copied // (1024*1024)}MB/{total_size // (1024*1024)}MB)')
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
        with log_lock:
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
        with log_lock:
            logging.error(f"Error counting ready tasks: {e}")
        return None

def normalize_meta_dict(meta):
    # Always ignore these fields for hashing
    ignore_fields = {"sourceDataUpdateDate", "buildDate"}
    norm = {}
    for k, v in meta.items():
        if k in ignore_fields:
            continue
        if isinstance(v, str):
            v_clean = v.replace('"', '').replace("'", "").replace(',', '').strip().lower()
            v_clean = ' '.join(v_clean.split())  # Collapse whitespace
            norm[k] = v_clean
        else:
            norm[k] = v
    return norm

def hash_meta(meta: dict) -> str:
    meta_copy = {k: v for k, v in meta.items() if k not in EXCLUDED_META_FIELDS}
    meta_norm = normalize_meta_dict(meta_copy)
    meta_str = json.dumps(meta_norm, sort_keys=True)
    return hashlib.sha256(meta_str.encode('utf-8')).hexdigest()

def hash_meta_from_zip(zip_path: str, iso: str, level: str) -> str:
    target_file_name = f"geoBoundaries-{iso}-{level}-metaData.json"
    with zipfile.ZipFile(zip_path, 'r') as z:
        with z.open(target_file_name) as f:
            zip_meta = json.load(f)
    return hash_meta(zip_meta)

def process_metadata(args):
    import threading
    path, metaSearch, isoDetails = args
    thread_name = threading.current_thread().name
    with log_lock:
        logging.info(f"Thread {thread_name} started: path={path}, metaSearch={metaSearch}")
    # Extract ISO/ADM from path/metaSearch
    iso = None
    adm = None
    try:
        json_path = path + "/" + metaSearch[0]
        with open(json_path, encoding='utf-8', mode="r") as j:
            meta = json.load(j)
        iso = meta.get('boundaryISO', 'UNKNOWN')
        adm = meta.get('boundaryType', 'UNKNOWN')
    except Exception as e:
        # Fallback if meta can't be loaded
        iso = 'UNKNOWN'
        adm = 'UNKNOWN'
        with log_lock:
            logging.error(f"Thread {thread_name} failed to load meta: {e}")
    jsonDict = defaultdict(list)
    try:
        json_path = path + "/" + metaSearch[0]
        with open(json_path, encoding='utf-8', mode="r") as j:
            meta = json.load(j)
        isoMeta = isoDetails[isoDetails["Alpha-3code"] == meta['boundaryISO']]
        if isoMeta.empty:
            with log_lock:
                logging.error(f"isoMeta lookup failed: path={path}, meta['boundaryISO']={meta.get('boundaryISO')}, isoMeta is empty. isoDetails Alpha-3code values: {isoDetails['Alpha-3code'].unique()}")
        try:
            metaLine = '"' + meta['boundaryID'] + '","' + isoMeta["Name"].values[0] + '","' + meta['boundaryISO'] + '","' + meta['boundaryYear'] + '","' + meta["boundaryType"] + '","'
        except Exception as e:
            with log_lock:
                logging.error(f"Error accessing isoMeta['Name'].values[0]: path={path}, meta['boundaryISO']={meta.get('boundaryISO')}, error={e}, isoMeta={isoMeta}")
            raise
        if("boundaryCanonical" in meta and len(meta["boundaryCanonical"])>0):
            bndCan = meta["boundaryCanonical"]
            metaLine = metaLine + meta["boundaryCanonical"] + '","'
        else:
            metaLine = metaLine + 'Unknown","'
            bndCan = "Unknown"
        # Cleanup free-form text fields
        meta['licenseDetail'] = meta["licenseDetail"].replace(',','').replace('\\','').replace('"','')
        metaLine = metaLine + meta['boundarySource'] + '","' + meta['boundaryLicense'] + '","' + meta['licenseDetail'].replace("https//","").replace("https://","").replace("http//","").replace("http://","") + '","' + meta['licenseSource'].replace("https//","").replace("https://","").replace("http//","").replace("http://","")  + '","'
        metaLine = metaLine + meta['boundarySourceURL'].replace("https//","https://").replace("https://","").replace("http//","").replace("http://","")  + '","' + meta['sourceDataUpdateDate'] + '","' + meta["buildDate"] + '","'
        metaLine = metaLine + isoMeta["Continent"].values[0] + '","' + isoMeta["UNSDG-region"].values[0] + '","'
        metaLine = metaLine + isoMeta["UNSDG-subregion"].values[0] + '","' \
            + isoMeta["worldBankIncomeGroup"].values[0] + '","'
        metaLine = metaLine + str(meta["admUnitCount"]) + '","' + str(meta["meanVertices"]) + '","' + str(meta["minVertices"]) + '","' + str(meta["maxVertices"]) + '","'
        metaLine = metaLine + str(meta["meanPerimeterLengthKM"]) + '","' + str(meta["minPerimeterLengthKM"]) + '","' + str(meta["maxPerimeterLengthKM"]) + '","'
        metaLine = metaLine + str(meta["meanAreaSqKM"]) + '","' + str(meta["minAreaSqKM"]) + '","' + str(meta["maxAreaSqKM"]) + '",'
        metaLine = metaLine.replace("nan","")
        static_file_path = path + "/" + f"geoBoundaries-{meta['boundaryISO']}-{meta['boundaryType']}-all.zip"
        unique_static_link = hash_meta(meta)
        static_link_file = f"geoBoundaries-{meta['boundaryISO']}-{meta['boundaryType']}-all-{unique_static_link}.zip"
        dest_path = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/gbWeb/data/static/" + static_link_file
        need_copy = True
        # Simplified: Only check if destination file exists, hash in file name matches, and zip is valid
        if os.path.exists(dest_path):
            try:
                with zipfile.ZipFile(dest_path, 'r') as z:
                    # Try listing files to check zip validity
                    z.testzip()
                with log_lock:
                    logging.info(f"HASH: No copy needed for {dest_path} (file exists, hash in name matches, zip is valid)")
                need_copy = False
            except Exception as e:
                with log_lock:
                    logging.warning(f"HASH: Destination zip exists but is invalid or corrupted: {dest_path}, error: {e}. Will overwrite.")
        if need_copy:
            with log_lock:
                logging.info(f"HASH: Copying static file to {dest_path} (does not exist, hash in name does not match, or zip invalid)")
            try:
                fast_copy(static_file_path, dest_path)
                with log_lock:
                    logging.info(f"Copied static file to {dest_path}")
            except Exception as e:
                err_msg = f"Failed to copy static file {static_file_path}: {e}"
                with log_lock:
                    logging.error(err_msg)

        meta['staticDownloadLink'] = url_static_base + static_link_file
        metaLine = metaLine + meta['staticDownloadLink'] + '"\n'
        # Upsert into boundary_meta
        with log_lock:
            logging.info(f"Thread {thread_name}: upserting metadata for ISO={meta.get('boundaryISO','?')}, ADM={meta.get('boundaryType','?')}")
        try:
            with connect_to_db() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        INSERT INTO boundary_meta (
                            boundaryISO, boundaryType, boundaryID, boundaryYear, boundarySource, boundaryCanonical, boundaryLicense, licenseDetail, licenseSource, boundarySourceURL, sourceDataUpdateDate, buildDate, admUnitCount, meanVertices, minVertices, maxVertices, meanPerimeterLengthKM, maxPerimeterLengthKM, minPerimeterLengthKM, meanAreaSqKM, minAreaSqKM, maxAreaSqKM, staticdownloadlink
                        ) VALUES (
                            %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s
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
                            maxAreaSqKM=EXCLUDED.maxAreaSqKM,
                            staticdownloadlink=EXCLUDED.staticdownloadlink
                    """,
                        (
                            meta['boundaryISO'],
                            meta['boundaryType'],
                            meta['boundaryID'],
                            meta['boundaryYear'] if meta['boundaryYear'] else '',
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
                            float(meta['maxAreaSqKM']) if meta['maxAreaSqKM'] else None,
                            meta['staticDownloadLink']
                        )
                    )
                    conn.commit()
                    with log_lock:
                        logging.info(f"Thread {thread_name}: Upsert committed for ISO={meta.get('boundaryISO','?')}, ADM={meta.get('boundaryType','?')}")
        except Exception as e:
            with log_lock:
                logging.error(f"Failed to upsert meta for {meta['boundaryISO']}-{meta['boundaryType']}: {e}")
        # Compose API JSON
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
        api_json_path = os.path.join(api_dir, "index.json")
        with log_lock:
            logging.info(f"Thread {thread_name}: Writing API JSON to {api_json_path}")
        # No locking needed: each thread writes a unique API JSON file
        try:
            with open(api_json_path, "w", encoding="utf-8") as jf:
                json.dump(api_json, jf, ensure_ascii=False, indent=2)
        except Exception as e:
            err_msg = f"Failed to write API JSON {api_json_path}: {e}"
            with log_lock:
                logging.error(err_msg)
        jsonDict[meta['boundaryISO']].append(api_json)
        with log_lock:
            logging.info(f"Thread {thread_name} completed.")
        return meta['boundaryISO'], api_json
    except Exception as e:
        err_msg = f"Error processing {path}: {e}"
        with log_lock:
            logging.error(err_msg)
        return None

def main():
    with log_lock:
        logging.info("Starting main function.")
    # Ensure boundary_meta table exists
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                CREATE TABLE IF NOT EXISTS boundary_meta (
                  boundaryISO             text NOT NULL,
                  boundaryType            text NOT NULL,
                  boundaryID              text,
                  boundaryYear            text,
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
                  staticdownloadlink      text,
                  PRIMARY KEY (boundaryISO, boundaryType)
                );
                """)
                conn.commit()
        with log_lock:
            logging.info("Ensured boundary_meta table exists.")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to create boundary_meta table: {e}")
        raise
    # Load ISO codes
    try:
        with log_lock:
            logging.info("Loading ISO code details...")
        isoDetails = pd.read_csv("../../dta/iso_3166_1_alpha_3.csv", encoding='utf-8')
        with log_lock:
            logging.info("Loaded ISO code details.")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to load ISO codes: {e}")
        raise
    # Gather all metaData.json files
    with log_lock:
        logging.info("Searching for metadata files...")
    meta_tasks = []
    for (path, dirname, filenames) in os.walk(GB_DIR + "releaseData/gbOpen"):
        metaSearch = [x for x in filenames if re.search('metaData.json', x)]
        if len(metaSearch) == 1 and "ADM" in path:
            meta_tasks.append((path, metaSearch, isoDetails))
    with log_lock:
        logging.info(f"Found {len(meta_tasks)} metadata file groups.")
    # Parallel processing
    jsonDict = defaultdict(list)
    # Production-scale parallelism
    with ThreadPoolExecutor(max_workers=30) as executor:
        with log_lock:
            logging.info(f"Launching ThreadPoolExecutor for metadata processing with {len(meta_tasks)} tasks...")
        futures = {executor.submit(process_metadata, args): args for args in meta_tasks}
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                with log_lock:
                    logging.error(f"Thread raised an exception: {exc}")
        with log_lock:
            logging.info("All threads completed. Proceeding to CSV export.")
    # Export metadata table to CSV
    try:
        with log_lock:
            logging.info("Exporting metadata table to CSV...")
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
    SELECT boundaryid, boundaryiso, boundaryyear, boundarytype, boundarycanonical, boundarysource, boundarylicense, licensedetail, licensesource, boundarysourceurl, sourcedataupdatedate, builddate, admunitcount, meanvertices, minvertices, maxvertices, meanperimeterlengthkm, minperimeterlengthkm, maxperimeterlengthkm, meanareasqkm, minareasqkm, maxareasqkm
    FROM boundary_meta
    ORDER BY boundaryiso, boundarytype;
""")
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
        with open(outputMetaCSV, 'w', encoding='utf-8') as f:
            f.write("boundaryID,boundaryName,boundaryISO,boundaryYearRepresented,boundaryType,boundaryCanonical,boundarySource,boundaryLicense,licenseDetail,licenseSource,boundarySourceURL,sourceDataUpdateDate,buildDate,Continent,UNSDG-region,UNSDG-subregion,worldBankIncomeGroup,admUnitCount,meanVertices,minVertices,maxVertices,meanPerimeterLengthKM,minPerimeterLengthKM,maxPerimeterLengthKM,meanAreaSqKM,minAreaSqKM,maxAreaSqKM,staticDownloadLink\n")
            for row in rows:
                iso = row[colnames.index('boundaryiso')]
                iso_row = isoDetails[isoDetails["Alpha-3code"] == iso]
                f.write(",".join([
                    f'"{row[colnames.index("boundaryid")]}"',
                    f'"{iso_row["Name"].values[0] if not iso_row.empty else ""}"',
                    f'"{row[colnames.index("boundaryiso")]}"',
                    f'"{row[colnames.index("boundaryyear")] if row[colnames.index("boundaryyear")] is not None else ""}"',
                    f'"{row[colnames.index("boundarytype")]}"',
                    f'"{row[colnames.index("boundarycanonical")]}"',
                    f'"{row[colnames.index("boundarysource")]}"',
                    f'"{row[colnames.index("boundarylicense")]}"',
                    f'"{row[colnames.index("licensedetail")]}"',
                    f'"{row[colnames.index("licensesource")]}"',
                    f'"{row[colnames.index("boundarysourceurl")]}"',
                    f'"{row[colnames.index("sourcedataupdatedate")]}"',
                    f'"{row[colnames.index("builddate")]}"',
                    f'"{iso_row["Continent"].values[0] if not iso_row.empty else ""}"',
                    f'"{iso_row["UNSDG-region"].values[0] if not iso_row.empty else ""}"',
                    f'"{iso_row["UNSDG-subregion"].values[0] if not iso_row.empty else ""}"',
                    f'"{iso_row["worldBankIncomeGroup"].values[0] if not iso_row.empty else ""}"',
                    f'"{row[colnames.index("admunitcount")]}"',
                    f'"{row[colnames.index("meanvertices")]}"',
                    f'"{row[colnames.index("minvertices")]}"',
                    f'"{row[colnames.index("maxvertices")]}"',
                    f'"{row[colnames.index("meanperimeterlengthkm")]}"',
                    f'"{row[colnames.index("minperimeterlengthkm")]}"',
                    f'"{row[colnames.index("maxperimeterlengthkm")]}"',
                    f'"{row[colnames.index("meanareasqkm")]}"',
                    f'"{row[colnames.index("minareasqkm")]}"',
                    f'"{row[colnames.index("maxareasqkm")]}"',
                    '""'
                ]) + "\n")
        with log_lock:
            logging.info(f"Exported metadata table to {outputMetaCSV}")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to export metadata table to CSV: {e}")
    # Build API index.json files
    try:
        # Build the "ALL" cases for each ISO in the API
        for iso, adm_entries in jsonDict.items():
            out_dir = os.path.join(apiPath, iso, "ALL")
            os.makedirs(out_dir, exist_ok=True)
            out_path = os.path.join(out_dir, "index.json")
            with open(out_path, "w", encoding="utf-8") as f:
                json.dump(adm_entries, f, ensure_ascii=False, indent=2)
        # ALL/ALL and ALL/ADM* - build from boundary_meta DB
        import psycopg2.extras
        adm_levels = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
        try:
            with connect_to_db() as conn:
                with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                    # ALL/ALL
                    cur.execute("SELECT * FROM boundary_meta")
                    all_rows = cur.fetchall()
                    global_all_dir = os.path.join(apiPath, "ALL")
                    global_all_all = os.path.join(global_all_dir, "ALL")
                    os.makedirs(global_all_dir, exist_ok=True)
                    os.makedirs(global_all_all, exist_ok=True)
                    global_all_path = os.path.join(global_all_all, "index.json")
                    # Define the expected field order and capitalization
                    api_fields = [
                        "boundaryID", "boundaryName", "boundaryISO", "boundaryYearRepresented", "boundaryType", "boundaryCanonical", "boundarySource", "boundaryLicense", "licenseDetail", "licenseSource", "boundarySourceURL", "sourceDataUpdateDate", "buildDate", "Continent", "UNSDG-region", "UNSDG-subregion", "worldBankIncomeGroup", "admUnitCount", "meanVertices", "minVertices", "maxVertices", "meanPerimeterLengthKM", "minPerimeterLengthKM", "maxPerimeterLengthKM", "meanAreaSqKM", "minAreaSqKM", "maxAreaSqKM", "staticDownloadLink", "gjDownloadURL", "tjDownloadURL", "imagePreview", "simplifiedGeometryGeoJSON"
                    ]
                    # Helper to map DB row to API JSON format (requires isoMeta for boundaryName, etc.)
                    def db_row_to_api_json(row):
                        # If you have a lookup for boundaryName/Continent/etc. from ISO, do it here; for now, use DB fields or empty string
                        return {
                            "boundaryID": row.get("boundaryid", ""),
                            "boundaryName": row.get("boundarycanonical", ""),  # Placeholder, ideally use ISO lookup
                            "boundaryISO": row.get("boundaryiso", ""),
                            "boundaryYearRepresented": row.get("boundaryyear", ""),
                            "boundaryType": row.get("boundarytype", ""),
                            "boundaryCanonical": row.get("boundarycanonical", ""),
                            "boundarySource": row.get("boundarysource", ""),
                            "boundaryLicense": row.get("boundarylicense", ""),
                            "licenseDetail": row.get("licensedetail", ""),
                            "licenseSource": row.get("licensesource", ""),
                            "boundarySourceURL": row.get("boundarysourceurl", ""),
                            "sourceDataUpdateDate": row.get("sourcedataupdatedate", ""),
                            "buildDate": row.get("builddate", ""),
                            "Continent": row.get("continent", ""),
                            "UNSDG-region": row.get("unsdg-region", ""),
                            "UNSDG-subregion": row.get("unsdg-subregion", ""),
                            "worldBankIncomeGroup": row.get("worldbankincomegroup", ""),
                            "admUnitCount": str(row.get("admunitcount", "")),
                            "meanVertices": str(row.get("meanvertices", "")),
                            "minVertices": str(row.get("minvertices", "")),
                            "maxVertices": str(row.get("maxvertices", "")),
                            "meanPerimeterLengthKM": str(row.get("meanperimeterlengthkm", "")),
                            "minPerimeterLengthKM": str(row.get("minperimeterlengthkm", "")),
                            "maxPerimeterLengthKM": str(row.get("maxperimeterlengthkm", "")),
                            "meanAreaSqKM": str(row.get("meanareasqkm", "")),
                            "minAreaSqKM": str(row.get("minareasqkm", "")),
                            "maxAreaSqKM": str(row.get("maxareasqkm", "")),
                            "staticDownloadLink": row.get("staticdownloadlink", ""),
                            "gjDownloadURL": row.get("gjdownloadurl", ""),
                            "tjDownloadURL": row.get("tjdownloadurl", ""),
                            "imagePreview": row.get("imagepreview", ""),
                            "simplifiedGeometryGeoJSON": row.get("simplifiedgeometrygeojson", "")
                        }
                    # ALL/ALL
                    with open(global_all_path, "w", encoding="utf-8") as f:
                        json.dump([db_row_to_api_json(r) for r in all_rows], f, ensure_ascii=False, indent=2)
                    # ALL/ADMX
                    for adm in adm_levels:
                        cur.execute("SELECT * FROM boundary_meta WHERE boundarytype = %s", (adm,))
                        adm_rows = cur.fetchall()
                        out_dir = os.path.join(apiPath, "ALL", adm)
                        os.makedirs(out_dir, exist_ok=True)
                        out_path = os.path.join(out_dir, "index.json")
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump([db_row_to_api_json(r) for r in adm_rows], f, ensure_ascii=False, indent=2)
            with log_lock:
                logging.info("ALL/ALL and ALL/ADM* API index.json files built from boundary_meta DB.")
        except Exception as e:
            with log_lock:
                logging.error(f"Failed to build ALL/ALL and ALL/ADM* API index.json files from DB: {e}")
        with log_lock:
            logging.info("API index.json files built.")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to build API index.json files: {e}")
    # Update DB status to indicate CSV construction is complete
    success_msg = "CSV and API Construction complete"
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute('UPDATE status SET "STATUS" = %s, "TIME" = NOW() WHERE "STATUS_TYPE" = %s', (success_msg, "FULL_DB_BUILD"))
                conn.commit()
        with log_lock:
            logging.info(f"Updated status table: {success_msg}")
    except Exception as db_exc:
        with log_lock:
            logging.error(f"Failed to update status table with success message: {db_exc}")
    with log_lock:
        logging.info("Processing complete.")

if __name__ == "__main__":
    main()
