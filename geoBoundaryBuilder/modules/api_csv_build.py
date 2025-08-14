import os
import sys
import logging
import threading
import json
import psycopg2
import pandas as pd
import psycopg2.extras

def connect_to_db():
    # You may want to import DB config from a shared location
    return psycopg2.connect(
        host=os.environ.get('DB_SERVICE', 'geoboundaries-postgres-service'),
        dbname=os.environ.get('DB_NAME', 'geoboundaries'),
        user=os.environ.get('DB_USER', 'geoboundaries'),
        password=os.environ.get('DB_PASSWORD', ''),
        port=os.environ.get('DB_PORT', 5432)
    )

# Paths (should match those used in threaded_build_csv_api.py)
outputMetaCSV = "/sciclone/geograd/geoBoundaries/database/geoBoundariesDev/releaseData/geoBoundariesOpen-meta.csv"
apiPath = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/gbWeb/api/current/gbOpen/"

log_dir = "/sciclone/geograd/geoBoundaries/logs/api_csv_build/"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "api_csv_build.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.FileHandler(log_file)]
)
log_lock = threading.Lock()

def export_metadata_csv():
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT * FROM boundary_meta")
                rows = cur.fetchall()
                colnames = [desc[0] for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                df.to_csv(outputMetaCSV, index=False)
        with log_lock:
            logging.info(f"Exported metadata table to {outputMetaCSV}")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to export metadata table to CSV: {e}")

def build_api_index_json():
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
                with open(global_all_path, "w", encoding="utf-8") as f:
                    json.dump(all_rows, f, ensure_ascii=False, indent=2)
                # ALL/ADM* (per admin level)
                for adm in adm_levels:
                    cur.execute("SELECT * FROM boundary_meta WHERE boundaryType = %s", (adm,))
                    adm_rows = cur.fetchall()
                    adm_dir = os.path.join(global_all_dir, adm)
                    os.makedirs(adm_dir, exist_ok=True)
                    adm_path = os.path.join(adm_dir, "index.json")
                    with open(adm_path, "w", encoding="utf-8") as f:
                        json.dump(adm_rows, f, ensure_ascii=False, indent=2)
                # Per-ISO/ALL (for each ISO)
                cur.execute("SELECT DISTINCT boundaryISO FROM boundary_meta")
                all_isos = [row["boundaryISO"] for row in cur.fetchall()]
                for iso in all_isos:
                    cur.execute("SELECT * FROM boundary_meta WHERE boundaryISO = %s", (iso,))
                    iso_rows = cur.fetchall()
                    out_dir = os.path.join(apiPath, iso, "ALL")
                    os.makedirs(out_dir, exist_ok=True)
                    out_path = os.path.join(out_dir, "index.json")
                    with open(out_path, "w", encoding="utf-8") as f:
                        json.dump(iso_rows, f, ensure_ascii=False, indent=2)
        with log_lock:
            logging.info(f"API index.json files built in {apiPath}")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to build API index.json files: {e}")

def main():
    export_metadata_csv()
    build_api_index_json()

if __name__ == "__main__":
    main()
