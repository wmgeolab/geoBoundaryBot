import sys
import os
import zipfile
import time
import psycopg2
from datetime import datetime
import logging
import pandas as pd
import shutil

# Import the builder class
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from builder_class import builder

# Paths
GB_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundariesDev/"
TMP_DIR = "/sciclone/geograd/geoBoundaries/tmp/gbBuilder/"
LOG_DIR = "/sciclone/geograd/geoBoundaries/logs/worker_script/"
META_DIR = "/sciclone/geograd/geoBoundaries/geoBoundaryBot/dta/"
os.makedirs(LOG_DIR, exist_ok=True)  # Ensure log directory exists

# Extract ISO, ADM, and taskid parameters
iso = sys.argv[1]
adm = sys.argv[2]
taskid = sys.argv[3]

# Configure logging with a centralized log file
log_file = os.path.join(LOG_DIR, f"worker_script_{iso}_{adm}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Optional: also log to console
    ]
)

def connect_to_db(max_attempts=10, retry_delay=15):
    """
    Establishes a connection to the PostGIS database with retry mechanism.
    """
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.environ.get('DB_NAME', 'geoboundaries'),
                user=os.environ.get('DB_USER', 'geoboundaries'),
                password=os.environ.get('DB_PASSWORD', ''),
                host=os.environ.get('DB_SERVICE', 'geoboundaries-postgres-service'),
                port=os.environ.get('DB_PORT', '5432')
            )
            logging.info("Database connection established.")
            return conn
        except Exception as e:
            logging.error(f"Database connection attempt {attempt} failed: {e}")
            if attempt < max_attempts:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("Failed to connect to database after maximum attempts.")
                raise

def update_status_in_db(conn, status_type, status, timestamp):
    """
    Update or insert a status record in the worker_status table
    """
    try:
        with conn.cursor() as cur:
            # Upsert query to either update existing row or insert new row in worker_status table
            upsert_query = """
            INSERT INTO worker_status ("STATUS_TYPE", "STATUS", "TIME") 
            VALUES (%s, %s, %s)
            ON CONFLICT ("STATUS_TYPE") 
            DO UPDATE SET 
                "STATUS" = EXCLUDED."STATUS", 
                "TIME" = EXCLUDED."TIME"
            """
            cur.execute(upsert_query, (status_type, status, timestamp))
            conn.commit()
            logging.info(f"Updated worker_status for {status_type}: {status}")
    except Exception as e:
        logging.error(f"Error updating worker_status in database: {e}")
        conn.rollback()

def update_tasks_db(conn, iso, adm, error_message):
    """
    Update tasks database with error message
    """
    try:
        with conn.cursor() as cur:
            update_query = """
            UPDATE tasks
            SET error_message = %s
            WHERE iso = %s AND adm = %s
            """
            cur.execute(update_query, (error_message, iso, adm))
            conn.commit()
            logging.info(f"Updated tasks database for {iso} {adm}: {error_message}")
    except Exception as e:
        logging.error(f"Error updating tasks database: {e}")
        conn.rollback()

# Create status type from ISO and ADM
status_type = f"{iso}_{adm}_WORKER"

# Load valid ISOs and Licenses
countries = pd.read_csv(os.path.join(os.path.dirname(__file__), META_DIR + "iso_3166_1_alpha_3.csv"))
isoList = countries["Alpha-3code"].values

licenses = pd.read_csv(os.path.join(os.path.dirname(__file__), META_DIR + "gbLicenses.csv"))
licenseList = licenses["license_name"].values

# File to process
filename = f"{iso}_{adm}.zip"
file_path = os.path.join(GB_DIR, "sourceData/gbOpen", filename)

try:
    # Establish database connection
    conn = connect_to_db()

    # Update status to indicate start of processing
    current_timestamp = datetime.now()
    update_status_in_db(conn, status_type, "STARTING_WORKER_SCRIPT", current_timestamp)

    # Process the single product type
    build_results = []
    try:
        logging.info(f"Processing {iso} {adm} for gbOpen")
        update_status_in_db(conn, status_type, "PROCESSING", datetime.now())

        # Initialize builder
        bnd = builder(iso, adm, "gbOpen", GB_DIR, LOG_DIR, TMP_DIR, isoList, licenseList)
        
        # Run builder stages
        stages = [
            ("checkExistence", "CHECKING_EXISTENCE"),
            ("checkSourceValidity", "VALIDATING_SOURCE"),
            ("checkBuildTabularMetaData", "BUILDING_METADATA"),
            ("checkBuildGeometryFiles", "PROCESSING_GEOMETRY"),
            ("calculateGeomMeta", "CALCULATING_GEOMETRY_META"),
            ("constructFiles", "CONSTRUCTING_FILES")
        ]

        for stage_method, stage_status in stages:
            logging.info(f"Running stage: {stage_method}")
            update_status_in_db(conn, status_type, stage_status, datetime.now())
            
            # Call the stage method dynamically
            method = getattr(bnd, stage_method)
            result = method()
            
            if "ERROR" in str(result):
                logging.error(f"Error in {stage_method}: {result}")
                update_status_in_db(conn, status_type, f"ERROR_{stage_status}", datetime.now())
                # Update tasks database with the error
                with conn.cursor() as cur:
                    update_query = """
                    UPDATE tasks
                    SET status = 'ERROR',
                        status_time = %s,
                        status_detail = %s
                    WHERE iso = %s AND adm = %s
                    """
                    cur.execute(update_query, (datetime.now(), str(result), iso, adm))
                    conn.commit()
                build_results.append([iso, adm, "gbOpen", result, f"E{stage_status[0]}"])
                break
        else:
            # If all stages complete successfully
            build_results.append([iso, adm, "gbOpen", "Successfully built.", "D"])
            update_status_in_db(conn, status_type, "BUILD_COMPLETE", datetime.now())

    except Exception as product_error:
        error_msg = f"Error processing gbOpen: {product_error}"
        logging.error(error_msg)
        error_code = "EP"  # Error Processing
        build_results.append([iso, adm, "gbOpen", str(product_error), error_code])
        
        # Update both status tables
        with conn.cursor() as cur:
            # Update tasks table
            update_query = """
            UPDATE tasks
            SET status = 'ERROR',
                status_time = %s,
                status_detail = %s
            WHERE taskid = %s
            """
            cur.execute(update_query, (datetime.now(), error_msg, taskid))
            
            # Update worker status
            status_msg = f"ERROR_{error_code}: {error_msg}"
            update_status_in_db(conn, status_type, status_msg, datetime.now())
            
            conn.commit()

    # Final status update with error code if present
    if build_results:
        result = build_results[-1]
        if len(result) >= 5 and result[4].startswith('E'):  # Check if there's an error code
            final_status = f"ERROR_{result[4]}: {result[3]}"  # Include both error code and message
        else:
            final_status = "COMPLETE"
        update_status_in_db(conn, status_type, final_status, datetime.now())
    
    # Log build results
    for result in build_results:
        logging.info(f"Build Result: {result}")

except Exception as main_error:
    logging.error(f"Critical error in worker script: {main_error}")
    update_status_in_db(conn, status_type, f"WORKER_SCRIPT_ERROR: {str(main_error)}", datetime.now())
    update_tasks_db(conn, iso, adm, str(main_error))
    raise

finally:
    # Close database connection if it was opened
    if 'conn' in locals() and conn:
        conn.close()

    # Sleep for 15 minutes after processing
    logging.info("Sleeping for 15 minutes...")
    time.sleep(900)  # 900 seconds = 15 minutes
