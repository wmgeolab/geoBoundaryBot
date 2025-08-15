import os
import sys
import time
import psycopg2
import logging
from datetime import datetime, timedelta
import hashlib
import pandas as pd
import re
import shutil
import zipfile
from concurrent.futures import ThreadPoolExecutor, as_completed
import json

#GB DIR:
GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/"
GB_WEB_DIR = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/gbWeb/"

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


def fast_copy(src, dst):
    """
    Copy a file from src to dst using shutil.copy2.
    Logs and prints when the copy starts and finishes, including the source and destination.
    """
    import shutil
    import os
    try:
        if os.path.exists(src) and os.path.exists(dst) and os.path.samefile(src, dst):
            with log_lock:
                logging.warning(f"COPY: Source and destination are the same file ({src}), skipping copy.")
            print(f"COPY: Source and destination are the same file ({src}), skipping copy.")
            return
    except Exception:
        pass  # os.path.samefile may fail if dst does not exist yet
    if not os.path.exists(src):
        with log_lock:
            logging.error(f"COPY: Source file does not exist: {src}")
        print(f"COPY: Source file does not exist: {src}")
        return
    with log_lock:
        logging.info(f"COPY: Starting copy from {src} to {dst}")
    print(f"COPY: Starting copy from {src} to {dst}")
    shutil.copy2(src, dst)
    with log_lock:
        logging.info(f"COPY: Finished copy from {src} to {dst}")
    print(f"COPY: Finished copy from {src} to {dst}")



def connect_to_db():
    return psycopg2.connect(
        host=DB_SERVICE,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        port=DB_PORT
    )

def get_metadata_static_status_row():
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute('SELECT "STATUS", "TIME" FROM status WHERE "STATUS_TYPE" = %s ORDER BY "TIME" DESC LIMIT 1', ("METADATA_STATIC_STATUS",))
                row = cur.fetchone()
                return row if row else (None, None)
    except Exception as e:
        with log_lock:
            logging.error(f"Error fetching METADATA_STATIC_STATUS row: {e}")
        return (None, None)

def update_metadata_static_status(status_msg):
    # Cache the last status to avoid unnecessary DB connections/updates
    if not hasattr(update_metadata_static_status, "_last_status"):
        update_metadata_static_status._last_status = None
    if status_msg == update_metadata_static_status._last_status:
        return  # Skip redundant DB update
    update_metadata_static_status._last_status = status_msg
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                # Try update first
                cur.execute('UPDATE status SET "STATUS" = %s, "TIME" = NOW() WHERE "STATUS_TYPE" = %s', (status_msg, "METADATA_STATIC_STATUS"))
                if cur.rowcount == 0:
                    # No row to update, insert
                    cur.execute('INSERT INTO status ("STATUS_TYPE", "STATUS", "TIME") VALUES (%s, %s, NOW())', ("METADATA_STATIC_STATUS", status_msg))
                conn.commit()
    except Exception as e:
        with log_lock:
            logging.error(f"Error updating METADATA_STATIC_STATUS: {e}")


def periodic_status_updater(stop_event, status_func, interval_sec=300):
    import threading
    while not stop_event.is_set():
        status_func()
        stop_event.wait(interval_sec)

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
    """Generate a hash of the metadata, including only specified fields."""
    # Define the fields to include in the hash
    included_fields = [
        'boundaryISO', 'boundaryType', 'boundaryID', 'boundaryYear', 
        'boundarySource', 'boundaryCanonical', 'boundaryLicense', 
        'licenseDetail', 'licenseSource', 'boundarySourceURL', 
        'admUnitCount', 'meanAreaSqKM'
    ]
    
    # Create a new dict with only the included fields
    meta_filtered = {k: meta.get(k) for k in included_fields if k in meta}
    
    # Normalize and hash the filtered metadata
    meta_norm = normalize_meta_dict(meta_filtered)
    meta_str = json.dumps(meta_norm, sort_keys=True)
    return hashlib.sha256(meta_str.encode('utf-8')).hexdigest()

def create_boundary_meta_table():
    """Create the boundary_meta table if it doesn't exist."""
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

def process_metadata(args):
    global files_done_counter
    import threading
    thread_start_time = time.time()
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

    try:
        # Timing: JSON loading
        json_load_start = time.time()
        json_path = path + "/" + metaSearch[0]
        with open(json_path, encoding='utf-8', mode="r") as j:
            meta = json.load(j)
        json_load_time = time.time() - json_load_start
        
        # Timing: ISO lookup
        iso_lookup_start = time.time()
        isoMeta = isoDetails[isoDetails["Alpha-3code"] == meta['boundaryISO']]
        iso_lookup_time = time.time() - iso_lookup_start
        if isoMeta.empty:
            with log_lock:
                logging.error(f"isoMeta lookup failed: path={path}, meta['boundaryISO']={meta.get('boundaryISO')}, isoMeta is empty. isoDetails Alpha-3code values: {isoDetails['Alpha-3code'].unique()}")
        
        # Timing: String processing and metadata line building
        string_processing_start = time.time()
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
        string_processing_time = time.time() - string_processing_start
        # Timing: File existence and validation check
        file_check_start = time.time()
        need_copy = True
        # Fast check: file exists, hash in name matches
        if os.path.exists(dest_path) and os.path.exists(static_file_path):
            need_copy = False
        else:
            need_copy = True
        file_check_time = time.time() - file_check_start
        
        # Timing: File copying (if needed)
        file_copy_start = time.time()
        if need_copy:
            #Update the current data for the API:
            # Copy all files from the built data folder to the web folder
            source_dir = os.path.join(GB_DIR, "releaseData", "gbOpen", meta['boundaryISO'], meta['boundaryType'])
            target_dir = os.path.join(GB_WEB_DIR, "data", "current", "gbOpen", meta['boundaryISO'], meta['boundaryType'])

            # Create target directory if it doesn't exist
            os.makedirs(target_dir, exist_ok=True)

            if os.path.exists(source_dir):
                # Copy all files from source to target
                for filename in os.listdir(source_dir):
                    source_file = os.path.join(source_dir, filename)
                    target_file = os.path.join(target_dir, filename)
                    try:
                        if os.path.isfile(source_file):
                            fast_copy(source_file, target_file)
                            with log_lock:
                                logging.info(f"Copied {source_file} to {target_file}")
                    except Exception as e:
                        with log_lock:
                            logging.error(f"Failed to copy {source_file} to {target_file}: {e}")
            else:
                with log_lock:
                    logging.warning(f"Source directory does not exist: {source_dir}")

            #Copy the static zip file
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
        file_copy_time = time.time() - file_copy_start if need_copy else 0.0

        meta['staticDownloadLink'] = url_static_base + static_link_file
        metaLine = metaLine + meta['staticDownloadLink'] + '"\n'
        # Upsert into boundary_meta
        with log_lock:
            logging.info(f"Thread {thread_name}: upserting metadata for ISO={meta.get('boundaryISO','?')}, ADM={meta.get('boundaryType','?')}")
        db_upsert_start = time.time()
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
                    db_upsert_time = time.time() - db_upsert_start
                    with log_lock:
                        logging.info(f"Thread {thread_name}: Upsert committed for ISO={meta.get('boundaryISO','?')}, ADM={meta.get('boundaryType','?')} (DB: {db_upsert_time:.3f}s)")
        except Exception as e:
            db_upsert_time = time.time() - db_upsert_start  # Capture timing even on error
            with log_lock:
                logging.error(f"Failed to upsert meta for {meta['boundaryISO']}-{meta['boundaryType']}: {e} (DB time: {db_upsert_time:.3f}s)")

        thread_total_time = time.time() - thread_start_time
        with log_lock:
            logging.info(f"Thread {thread_name} completed in {thread_total_time:.3f}s (JSON: {json_load_time:.3f}s, ISO: {iso_lookup_time:.3f}s, STR: {string_processing_time:.3f}s, CHK: {file_check_time:.3f}s, CPY: {file_copy_time:.3f}s, DB: {db_upsert_time:.3f}s)")
        with files_done_counter['lock']:
            files_done_counter['count'] += 1
        return meta['boundaryISO']
    except Exception as e:
        err_msg = f"Error processing {path}: {e}"
        with log_lock:
            logging.error(err_msg)
        return None

# Thread-safe counter for files done processing
files_done_counter = {'count': 0, 'lock': threading.Lock()}

def main():
    import time
    start_time = time.time()
    with log_lock:
        logging.info("=== METADATA STATIC BUILD STARTED ===")
    
    # Step 1: Check for ready tasks
    step1_start = time.time()
    ready_count = get_ready_task_count()
    step1_time = time.time() - step1_start
    with log_lock:
        logging.info(f"TIMING: Ready task count check took {step1_time:.2f} seconds")
    if ready_count is None:
        with log_lock:
            logging.error("Could not determine ready task count; aborting build.")
        return
    if ready_count > 0:
        with log_lock:
            logging.info(f"{ready_count} tasks are in 'ready' state. Metadata processing will not proceed.")
        update_metadata_static_status(f"Not started: {ready_count} tasks are in 'ready' state.")
        return

    # Step 2: 48-hour timer check
    step2_start = time.time()
    # Hardcoded time for testing
    from datetime import datetime, timedelta
    import pytz
    current_time = datetime(2025, 8, 12, 15, 35, 4, tzinfo=pytz.timezone('America/New_York'))
    last_status, last_time = get_metadata_static_status_row()
    step2_time = time.time() - step2_start
    with log_lock:
        logging.info(f"TIMING: 48-hour timer check took {step2_time:.2f} seconds")
    if last_time:
        # last_time is a datetime (assume UTC from DB)
        if isinstance(last_time, str):
            # Parse if string
            from dateutil import parser
            last_time_dt = parser.parse(last_time)
        else:
            last_time_dt = last_time
        # Convert last_time to the same tz as current_time
        if last_time_dt.tzinfo is None:
            last_time_dt = last_time_dt.replace(tzinfo=pytz.UTC)
        elapsed = (current_time - last_time_dt).total_seconds()
        if elapsed < 48 * 3600:
            with log_lock:
                logging.info(f"Metadata processing ran less than 48 hours ago (last at {last_time_dt}). Not running again.")
            return

    update_metadata_static_status("Starting metadata processing")

    # Step 3: Start periodic status updater
    step3_start = time.time()
    import threading
    stop_status_thread = threading.Event()
    def status_func():
        update_metadata_static_status("Metadata processing in progress")
        with log_lock:
            logging.info(f"{files_done_counter['count']} files done processing")
    status_thread = threading.Thread(target=periodic_status_updater, args=(stop_status_thread, status_func, 300))
    status_thread.start()
    step3_time = time.time() - step3_start
    with log_lock:
        logging.info(f"TIMING: Status updater setup took {step3_time:.2f} seconds")

    with log_lock:
        logging.info("Starting main function.")
    # Step 4: Create boundary_meta table if not exists
    step4_start = time.time()
    create_boundary_meta_table()
    step4_time = time.time() - step4_start
    with log_lock:
        logging.info(f"TIMING: Boundary meta table creation took {step4_time:.2f} seconds")

    # Step 5: Load ISO details
    step5_start = time.time()
    isoDetails = pd.read_csv("../../dta/iso_3166_1_alpha_3.csv")
    step5_time = time.time() - step5_start
    with log_lock:
        logging.info(f"TIMING: ISO details loading took {step5_time:.2f} seconds")

    # Gather all metaData.json files
    with log_lock:
        logging.info("Searching for metadata files...")
    # Step 6: Find metadata files
    step6_start = time.time()
    meta_tasks = []
    for root, dirs, files in os.walk(GB_DIR):
        path = root
        metaSearch = [f for f in files if "metaData.json" in f]
        if len(metaSearch) == 1 and "ADM" in path:
            meta_tasks.append((path, metaSearch, isoDetails))
    step6_time = time.time() - step6_start
    with log_lock:
        logging.info(f"TIMING: Metadata file discovery took {step6_time:.2f} seconds")
        logging.info(f"Found {len(meta_tasks)} metadata file groups.")

    # Parallel processing
    step7_start = time.time()
    with ThreadPoolExecutor(max_workers=30) as executor:
        with log_lock:
            logging.info(f"Launching ThreadPoolExecutor for metadata processing with {len(meta_tasks)} tasks...")
        futures_start = time.time()
        futures = {executor.submit(process_metadata, args): args for args in meta_tasks}
        futures_time = time.time() - futures_start
        with log_lock:
            logging.info(f"TIMING: Future submission took {futures_time:.2f} seconds")
        for future in as_completed(futures):
            try:
                future.result()
            except Exception as exc:
                with log_lock:
                    logging.error(f"Thread raised an exception: {exc}")
        processing_time = time.time() - step7_start
        with log_lock:
            logging.info(f"TIMING: All thread processing took {processing_time:.2f} seconds")
            logging.info("All threads completed. Metadata table populated.")

    # Final cleanup and status update
    cleanup_start = time.time()
    with log_lock:
        logging.info("Metadata processing and database population complete.")
    # Update DB status to indicate metadata table population is complete
    success_msg = "Metadata table population complete"
    update_metadata_static_status(success_msg)
    stop_status_thread.set()
    status_thread.join(timeout=10)
    cleanup_time = time.time() - cleanup_start
    
    total_time = time.time() - start_time
    with log_lock:
        logging.info(f"TIMING: Final cleanup took {cleanup_time:.2f} seconds")
        logging.info(f"TIMING: TOTAL METADATA BUILD TIME: {total_time:.2f} seconds")
        logging.info("=== METADATA STATIC BUILD COMPLETED ===")
        logging.info("Processing complete.")

if __name__ == "__main__":
    main()
