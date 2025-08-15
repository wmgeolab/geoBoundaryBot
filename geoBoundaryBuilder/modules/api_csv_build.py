import os
import json
import logging
import threading
import psycopg2
import pandas as pd
import psycopg2.extras
from decimal import Decimal
import pandas as pd
from datetime import datetime, date
from typing import Any, Dict, List, Union

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
                # Get column names and convert to camelCase
                colnames = [to_camel_case(desc[0]) for desc in cur.description]
                df = pd.DataFrame(rows, columns=colnames)
                df.to_csv(outputMetaCSV, index=False)
        with log_lock:
            logging.info(f"Exported metadata table to {outputMetaCSV}")
    except Exception as e:
        with log_lock:
            logging.error(f"Failed to export metadata table to CSV: {e}")

def to_camel_case(snake_str: str) -> str:
    """Convert snake_case to camelCase with specific exceptions for ID, ISO, URL, KM, Sq."""
    # Handle special cases first
    special_cases = {
        'boundaryiso': 'boundaryISO',
        'boundaryid': 'boundaryID',
        'boundaryurl': 'boundaryURL',
        'sqkm': 'SqKM',
        'km': 'KM'
    }
    
    # Check for special cases
    lower_str = snake_str.lower()
    if lower_str in special_cases:
        return special_cases[lower_str]
    
    # Handle boundarySource, boundaryType, etc.
    if snake_str.startswith('boundary'):
        return 'boundary' + snake_str[8:].title()
    
    # For other fields, convert to camelCase
    components = snake_str.split('_')
    return components[0].lower() + ''.join(x.title() for x in components[1:])

def convert_db_row_to_api_format(row: dict) -> dict:
    """Convert a database row to API format with proper field names and structure."""
    if not row:
        return {}
    
    # Base URL for downloads
    base_url = "https://www.geoboundaries-dev.org/data/current/gbOpen"
    
    # Extract base fields
    boundary_id = row.get('boundaryid', '')
    boundary_iso = row.get('boundaryiso', '')
    boundary_type = row.get('boundarytype', '').upper()
    
    # Create the output dictionary with all required fields
    result = {
        "boundaryID": boundary_id,
        "boundaryName": row.get('boundaryname', ''),
        "boundaryISO": boundary_iso,
        "boundaryYearRepresented": str(row.get('boundaryyear', '')),
        "boundaryType": boundary_type,
        "boundaryCanonical": row.get('boundarycanonical', 'Unknown'),
        "boundarySource": row.get('boundarysource', ''),
        "boundaryLicense": row.get('boundarylicense', ''),
        "licenseDetail": str(row.get('licensedetail', '')),
        "licenseSource": (row.get('licensesource', '')
                         .replace('https//', '')
                         .replace('http//', '')
                         .replace('https://', '')
                         .replace('http://', '')),
        "boundarySourceURL": (row.get('boundarysourceurl', '')
                             .replace('https//', '')
                             .replace('http//', '')
                             .replace('https://', '')
                             .replace('http://', '')),
        "sourceDataUpdateDate": row.get('sourcedataupdatedate', '').strftime('%a %b %d %H:%M:%S %Y') if row.get('sourcedataupdatedate') else '',
        "buildDate": row.get('builddate', '').strftime('%b %d, %Y') if row.get('builddate') else '',
        "Continent": row.get('continent', ''),
        "UNSDG-region": row.get('unsdg_region', ''),
        "UNSDG-subregion": row.get('unsdg_subregion', ''),
        "worldBankIncomeGroup": row.get('worldbankincomegroup', ''),
        "admUnitCount": str(row.get('admunitcount', '')),
        "meanVertices": str(row.get('meanvertices', '')),
        "minVertices": str(int(row.get('minvertices', 0))),
        "maxVertices": str(int(row.get('maxvertices', 0))),
        "meanPerimeterLengthKM": str(row.get('meanperimeterlengthkm', '')),
        "minPerimeterLengthKM": str(row.get('minperimeterlengthkm', '')),
        "maxPerimeterLengthKM": str(row.get('maxperimeterlengthkm', '')),
        "meanAreaSqKM": str(row.get('meanareasqkm', '')),
        "minAreaSqKM": str(row.get('minareasqkm', '')),
        "maxAreaSqKM": str(row.get('maxareasqkm', '')),
    }
    
    # Add download URLs if we have the required fields
    if boundary_iso and boundary_type:
        base_path = f"{boundary_iso}/{boundary_type}/geoBoundaries-{boundary_iso}-{boundary_type}"
        # Use the staticdownloadlink from the database if available, otherwise construct it
        static_download = row.get('staticdownloadlink')
        if not static_download:
            static_download = f"{base_url}/{base_path}-all.zip"
            
        result.update({
            "staticDownloadLink": static_download,
            "gjDownloadURL": f"{base_url}/{base_path}.geojson",
            "tjDownloadURL": f"{base_url}/{base_path}.topojson",
            "imagePreview": f"{base_url}/{base_path}-PREVIEW.png",
            "simplifiedGeometryGeoJSON": f"{base_url}/{base_path}_simplified.geojson"
        })
    
    # Handle None/NaN values
    for key, value in result.items():
        if pd.isna(value) or value is None:
            result[key] = ""
            
    return result

def convert_datetime_to_iso(obj: Any) -> Any:
    """Convert datetime, date, and Decimal objects to JSON-serializable types."""
    if obj is None:
        return None
    elif isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {k: convert_datetime_to_iso(v) for k, v in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_datetime_to_iso(item) for item in obj]
    return obj

def build_api_index_json():
    adm_levels = ["ADM0", "ADM1", "ADM2", "ADM3", "ADM4", "ADM5"]
    try:
        with connect_to_db() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
                # ALL/ALL
                cur.execute("SELECT * FROM boundary_meta")
                all_rows = [convert_datetime_to_iso(convert_db_row_to_api_format(row)) for row in cur.fetchall()]
                global_all_dir = os.path.join(apiPath, "ALL")
                global_all_all = os.path.join(global_all_dir, "ALL")
                os.makedirs(global_all_dir, exist_ok=True)
                os.makedirs(global_all_all, exist_ok=True)
                global_all_path = os.path.join(global_all_all, "index.json")
                with open(global_all_path, "w", encoding="utf-8") as f:
                    json.dump(all_rows, f, ensure_ascii=False, indent=2)
                # ALL/ADM* (per admin level)
                for adm in adm_levels:
                    cur.execute("SELECT * FROM boundary_meta WHERE boundarytype = %s", (adm.upper(),))
                    adm_rows = [convert_datetime_to_iso(convert_db_row_to_api_format(row)) for row in cur.fetchall()]
                    adm_dir = os.path.join(global_all_dir, adm)
                    os.makedirs(adm_dir, exist_ok=True)
                    adm_path = os.path.join(adm_dir, "index.json")
                    with open(adm_path, "w", encoding="utf-8") as f:
                        json.dump(adm_rows, f, ensure_ascii=False, indent=2)
                # Per-ISO/ALL (for each ISO)
                cur.execute("SELECT DISTINCT boundaryiso FROM boundary_meta WHERE boundaryiso IS NOT NULL")
                iso_rows = cur.fetchall()
                all_isos = [row["boundaryiso"] for row in iso_rows if row and "boundaryiso" in row and row["boundaryiso"]]
                
                if not all_isos:
                    with log_lock:
                        logging.warning("No boundaryISO values found in boundary_meta table")
                
                for iso in all_isos:
                    try:
                        # Get all records for this ISO
                        cur.execute("SELECT * FROM boundary_meta WHERE boundaryiso = %s", (iso,))
                        rows = cur.fetchall()
                        if not rows:
                            continue
                            
                        # Create ISO/ALL/index.json
                        iso_rows = [convert_datetime_to_iso(convert_db_row_to_api_format(row)) for row in rows]
                        out_dir = os.path.join(apiPath, str(iso), "ALL")
                        os.makedirs(out_dir, exist_ok=True)
                        out_path = os.path.join(out_dir, "index.json")
                        with open(out_path, "w", encoding="utf-8") as f:
                            json.dump(iso_rows, f, ensure_ascii=False, indent=2)
                        
                        # Create ISO/ADM*/index.json for each ADM level
                        for adm in adm_levels:
                            try:
                                # Find the row for this ADM level
                                adm_row = next(
                                    (row for row in rows if row.get("boundarytype", "").upper() == adm),
                                    None
                                )
                                
                                if not adm_row:
                                    continue
                                    
                                # Convert to API format and handle datetime/Decimal
                                adm_data = convert_datetime_to_iso(convert_db_row_to_api_format(adm_row))
                                
                                adm_dir = os.path.join(apiPath, str(iso), adm)
                                os.makedirs(adm_dir, exist_ok=True)
                                adm_path = os.path.join(adm_dir, "index.json")
                                with open(adm_path, "w", encoding="utf-8") as f:
                                    # Dump single object without array brackets
                                    json.dump(adm_data, f, ensure_ascii=False, indent=2)
                                    
                            except Exception as adm_error:
                                with log_lock:
                                    logging.error(f"Error processing {iso}/{adm}: {str(adm_error)}")
                                    
                    except Exception as e:
                        with log_lock:
                            logging.error(f"Error processing ISO {iso}: {str(e)}")
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
