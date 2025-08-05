import sys
import os
os.environ["OGR_GEOJSON_MAX_OBJ_SIZE"] = "0"
import zipfile
import time
import psycopg2
from datetime import datetime, timezone
import logging
import pandas as pd
import shutil
import json

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
        # Initialize source_date
        source_date = None
        
        # Extract ISO and ADM from status_type
        parts = status_type.split('_')
        logging.info(f"Status type parts: {parts}")
        
        if len(parts) >= 3 and parts[1].startswith('ADM'):
            iso = parts[0]
            adm = parts[1]
            logging.info(f"Processing ISO: {iso}, ADM: {adm}")
            
            # List possible metadata paths
            possible_paths = [
                f"/sciclone/geograd/geoBoundaries/database/geoBoundariesDev/releaseData/gbOpen/{iso}/{adm}/geoBoundaries-{iso}-{adm}-metaData.json"
            ]
            
            for meta_path in possible_paths:
                logging.info(f"Checking metadata path: {meta_path}")
                if os.path.exists(meta_path):
                    logging.info(f"Found metadata file at: {meta_path}")
                    try:
                        # Try different encodings if needed
                        encodings = ['utf-8', 'latin-1', 'cp1252']
                        content = None
                        
                        for encoding in encodings:
                            try:
                                with open(meta_path, 'r', encoding=encoding) as f:
                                    content = f.read()
                                    logging.info(f"Successfully read file with encoding: {encoding}")
                                    break
                            except UnicodeDecodeError:
                                logging.warning(f"Failed to read file with encoding: {encoding}")
                                continue
                        
                        if content is None:
                            logging.error(f"Could not read file with any encoding: {meta_path}")
                            continue
                        
                        logging.info(f"Metadata content: {content[:200]}...")  # Log first 200 chars
                        
                        try:
                            # Try to parse as JSON
                            meta_data = json.loads(content)
                        except json.JSONDecodeError as json_err:
                            logging.warning(f"JSON parsing error: {json_err}. Trying to read file line by line...")
                            # Try to extract sourceDataUpdateDate manually using regex
                            import re
                            source_date_match = re.search(r'"sourceDataUpdateDate"\s*:\s*"([^"]+)"', content)
                            if source_date_match:
                                source_date_str = source_date_match.group(1)
                                logging.info(f"Extracted sourceDataUpdateDate using regex: {source_date_str}")
                                try:
                                    # Try different date formats
                                    for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d', '%b %d, %Y', '%a %b %d %H:%M:%S %Y']:
                                        try:
                                            source_date = datetime.strptime(source_date_str, fmt).replace(tzinfo=timezone.utc)
                                            logging.info(f"Successfully parsed source date as: {source_date}")
                                            break
                                        except ValueError:
                                            continue
                                    
                                    # If none of the formats worked, try a manual approach for common formats
                                    if not source_date:
                                        try:
                                            # Try to handle format like "Wed Dec 18 10:12:38 2024"
                                            # Format: Weekday Month Day HH:MM:SS Year
                                            parts = source_date_str.split()
                                            if len(parts) == 5:  # Weekday Month Day HH:MM:SS Year
                                                weekday, month, day, time_str, year = parts
                                                month_map = {
                                                    'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                                                    'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                                }
                                                month_num = month_map.get(month)
                                                if month_num:
                                                    hour, minute, second = map(int, time_str.split(':'))
                                                    source_date = datetime(int(year), month_num, int(day), hour, minute, second).replace(tzinfo=timezone.utc)
                                                    logging.info(f"Successfully parsed source date using manual parsing: {source_date}")
                                        except Exception as e:
                                            logging.warning(f"Error in manual date parsing: {e}")
                                except Exception as e:
                                    logging.warning(f"Error parsing source date '{source_date_str}': {e}")
                            continue  # Skip the rest of the JSON processing
                        
                        # First check for sourceDataUpdateDate field directly
                        if 'sourceDataUpdateDate' in meta_data:
                            source_date_str = meta_data.get('sourceDataUpdateDate')
                            logging.info(f"Found sourceDataUpdateDate in metadata: {source_date_str}")
                            try:
                                # Try different date formats
                                for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d', '%b %d, %Y', '%a %b %d %H:%M:%S %Y']:
                                    try:
                                        source_date = datetime.strptime(source_date_str, fmt).replace(tzinfo=timezone.utc)
                                        logging.info(f"Successfully parsed source date as: {source_date}")
                                        break
                                    except ValueError:
                                        continue
                                
                                # If none of the formats worked, try a manual approach for common formats
                                if not source_date:
                                    try:
                                        # Try to handle format like "Wed Dec 18 10:12:38 2024"
                                        # Format: Weekday Month Day HH:MM:SS Year
                                        parts = source_date_str.split()
                                        if len(parts) == 5:  # Weekday Month Day HH:MM:SS Year
                                            weekday, month, day, time_str, year = parts
                                            month_map = {
                                                'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                                                'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                            }
                                            month_num = month_map.get(month)
                                            if month_num:
                                                hour, minute, second = map(int, time_str.split(':'))
                                                source_date = datetime(int(year), month_num, int(day), hour, minute, second).replace(tzinfo=timezone.utc)
                                                logging.info(f"Successfully parsed source date using manual parsing: {source_date}")
                                    except Exception as e:
                                        logging.warning(f"Error in manual date parsing: {e}")
                            except Exception as e:
                                logging.warning(f"Error parsing source date '{source_date_str}': {e}")
                        
                        # If sourceDataUpdateDate not found, try other possible date fields
                        if not source_date:
                            date_fields = ['sourceDate', 'sourceDataDate', 'lastUpdated', 'date']
                            for field in date_fields:
                                source_date_str = meta_data.get(field)
                                if source_date_str:
                                    logging.info(f"Found source date in field '{field}': {source_date_str}")
                                    try:
                                        # Try different date formats
                                        for fmt in ['%Y-%m-%d', '%Y%m%d', '%Y/%m/%d', '%b %d, %Y', '%a %b %d %H:%M:%S %Y']:
                                            try:
                                                source_date = datetime.strptime(source_date_str, fmt).replace(tzinfo=timezone.utc)
                                                logging.info(f"Successfully parsed source date as: {source_date}")
                                                break
                                            except ValueError:
                                                continue
                                        
                                        # If none of the formats worked, try a manual approach for common formats
                                        if not source_date:
                                            try:
                                                # Try to handle format like "Wed Dec 18 10:12:38 2024"
                                                # Format: Weekday Month Day HH:MM:SS Year
                                                parts = source_date_str.split()
                                                if len(parts) == 5:  # Weekday Month Day HH:MM:SS Year
                                                    weekday, month, day, time_str, year = parts
                                                    month_map = {
                                                        'Jan': 1, 'Feb': 2, 'Mar': 3, 'Apr': 4, 'May': 5, 'Jun': 6,
                                                        'Jul': 7, 'Aug': 8, 'Sep': 9, 'Oct': 10, 'Nov': 11, 'Dec': 12
                                                    }
                                                    month_num = month_map.get(month)
                                                    if month_num:
                                                        hour, minute, second = map(int, time_str.split(':'))
                                                        source_date = datetime(int(year), month_num, int(day), hour, minute, second).replace(tzinfo=timezone.utc)
                                                        logging.info(f"Successfully parsed source date using manual parsing: {source_date}")
                                            except Exception as e:
                                                logging.warning(f"Error in manual date parsing: {e}")
                                        if source_date:
                                            break
                                    except Exception as e:
                                        logging.warning(f"Error parsing source date '{source_date_str}': {e}")
                        
                        if source_date:
                            break  # Exit the file loop if we found a date
                    except Exception as e:
                        logging.warning(f"Error reading metadata file {meta_path}: {e}")
                else:
                    logging.info(f"Metadata file not found at: {meta_path}")
        
        with conn.cursor() as cur:
            # First, try to alter table to add SOURCE_DATE column if it doesn't exist
            try:
                cur.execute("""
                    ALTER TABLE worker_status 
                    ADD COLUMN IF NOT EXISTS "SOURCE_DATE" TIMESTAMP WITH TIME ZONE
                """)
                conn.commit()
            except Exception as e:
                logging.warning(f"Could not add SOURCE_DATE column: {e}")
                conn.rollback()
            
            # Debug log the values being inserted
            logging.info(f"Inserting values - STATUS_TYPE: {status_type}, STATUS: {status}, TIME: {timestamp}, SOURCE_DATE: {source_date}")
            
            # Upsert query including SOURCE_DATE
            upsert_query = """
            INSERT INTO worker_status ("STATUS_TYPE", "STATUS", "TIME", "SOURCE_DATE") 
            VALUES (%s, %s, %s, %s)
            ON CONFLICT ("STATUS_TYPE") 
            DO UPDATE SET 
                "STATUS" = EXCLUDED."STATUS", 
                "TIME" = EXCLUDED."TIME",
                "SOURCE_DATE" = EXCLUDED."SOURCE_DATE"
            """
            
            # Execute the query and verify the result
            cur.execute(upsert_query, (status_type, status, timestamp, source_date))
            conn.commit()
            
            # Verify the update
            cur.execute('SELECT "SOURCE_DATE" FROM worker_status WHERE "STATUS_TYPE" = %s', (status_type,))
            result = cur.fetchone()
            logging.info(f"Verified SOURCE_DATE after update for {status_type}: {result[0] if result else None}")
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

def format_elapsed_time(start_time):
    """Format elapsed time in human readable format"""
    elapsed = datetime.now() - start_time
    hours, remainder = divmod(int(elapsed.total_seconds()), 3600)
    minutes, seconds = divmod(remainder, 60)
    return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

try:
    # Establish database connection
    conn = connect_to_db()

    # Record start time for elapsed time calculation
    start_time = datetime.now()
    update_status_in_db(conn, status_type, "STARTING_WORKER_SCRIPT", start_time)

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
            ("calculateGeomMeta", "CALCULATING_GEOMETRY_META")
        ]
        
        # Force rebuild by setting changesDetected to True
        bnd.changesDetected = True
        stages.append(("constructFiles", "CONSTRUCTING_FILES"))

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
            
            # Calculate elapsed time
            elapsed_time = format_elapsed_time(start_time)
            
            # Update both worker_status and tasks tables
            with conn.cursor() as cur:
                # Update worker status with completion time
                update_status_in_db(conn, status_type, f"COMPLETE: {elapsed_time}", datetime.now())
                
                # Update tasks table
                update_query = """
                UPDATE tasks
                SET status = 'COMPLETE',
                    status_time = %s,
                    status_detail = %s
                WHERE taskid = %s
                """
                cur.execute(update_query, (datetime.now(), f"Completed in {elapsed_time}", taskid))
                conn.commit()

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
