import os
import subprocess
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime, timedelta
import time
import uuid  # For generating unique task IDs
import logging  # For logging functionality

# Set up logging
log_dir = "/sciclone/geograd/geoBoundaries/logs/queue_operator/"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "queue_operator.log")

# Configure logging
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler(log_file)])

# Database Configuration
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""  # Trust-based auth, no password
DB_PORT = 5432

# Task Directory
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"


def connect_to_db(max_attempts=10, retry_delay=15):
    """
    Establishes a connection to the PostGIS database with retry mechanism.
    
    Args:
        max_attempts (int): Maximum number of connection attempts
        retry_delay (int): Delay between connection attempts in seconds
    """
    for attempt in range(1, max_attempts + 1):
        try:
            conn = psycopg2.connect(
                dbname=os.environ.get('DB_NAME', 'geoboundaries'),
                user=os.environ.get('DB_USER', 'postgres'),
                password=os.environ.get('DB_PASSWORD', ''),
                host=os.environ.get('DB_SERVICE', 'localhost'),
                port=os.environ.get('DB_PORT', '5432')
            )
            logging.info("Database connection established.")
            return conn
        except Exception as e:
            logging.warning(f"Database connection attempt {attempt} failed: {e}")
            
            if attempt < max_attempts:
                logging.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
            else:
                logging.error("All database connection attempts failed.")
                sys.exit(1)


def create_tasks_table(conn):
    """Creates the 'Tasks' table if it does not already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Tasks (
        taskID VARCHAR(36) PRIMARY KEY,  -- UUID for unique task ID
        ISO VARCHAR(10),
        ADM VARCHAR(10),
        time_added TIMESTAMP,
        time_completed TIMESTAMP,
        runtime INTERVAL,
        filesize BIGINT,
        status VARCHAR(20),
        status_detail TEXT,
        status_time TIMESTAMP
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    logging.info("Table 'Tasks' created or verified successfully.")


def populate_tasks_table(conn):
    """Iterates over files in the TASK_DIR and populates the 'Tasks' table."""
    tasks_added = 0
    with conn.cursor() as cur:
        for filename in os.listdir(TASK_DIR):
            if filename.endswith(".zip"):
                try:
                    # Parse ISO and ADM from filename
                    iso, adm = filename.split(".zip")[0].split("_")
                    file_path = os.path.join(TASK_DIR, filename)
                    file_size = os.path.getsize(file_path)  # File size in bytes
                    task_id = str(uuid.uuid4())  # Generate a unique task ID

                    # Check if the record already exists
                    check_query = sql.SQL("SELECT COUNT(*) FROM Tasks WHERE ISO = %s AND ADM = %s AND status = 'ready'").format(
                        sql.Identifier('Tasks'))
                    cur.execute(check_query, (iso, adm))
                    exists = cur.fetchone()[0]

                    if exists > 0:
                        logging.info(f"Record for ISO: {iso}, ADM: {adm} already exists and is ready. Skipping insertion.")
                        continue  # Skip to the next file

                    # Insert a new task into the table
                    insert_query = sql.SQL("""
                        INSERT INTO Tasks (taskID, ISO, ADM, time_added, filesize, status, status_time)
                        VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """)
                    cur.execute(insert_query, (task_id, iso, adm, datetime.now(), file_size, "ready", datetime.now()))
                    tasks_added += 1
                    logging.info(f"Task {task_id} for {filename} added successfully.")

                except Exception as e:
                    logging.error(f"Error processing file {filename}: {e}")
        conn.commit()
    return tasks_added


def get_last_queue_status_time(conn):
    """
    Retrieve the timestamp of the last QUEUE_STATUS update.
    
    Args:
        conn (psycopg2.connection): Database connection
    
    Returns:
        datetime: Timestamp of the last QUEUE_STATUS, or current time if not found
    """
    try:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT "TIME" 
                FROM status 
                WHERE "STATUS_TYPE" = 'QUEUE_STATUS' 
                ORDER BY "TIME" DESC 
                LIMIT 1
            """)
            result = cur.fetchone()
            
            if result:
                return result[0]
            else:
                # If no previous status, use current time
                return datetime.now()
    except Exception as e:
        logging.error(f"Error retrieving last queue status time: {e}")
        return datetime.now()


if __name__ == "__main__":
    logging.info("Script started.")
    
    # Retrieve initial last queue time from database
    with connect_to_db() as initial_conn:
        last_queue_time = get_last_queue_status_time(initial_conn)
    
    queue_interval = timedelta(hours=1)
    
    while True:
        current_time = datetime.now()
        
        # Calculate time until next queue population based on last queue time
        time_until_next_queue = queue_interval - (current_time - last_queue_time)
        
        # Update queue heartbeat in database
        try:
            with connect_to_db() as heartbeat_conn:
                with heartbeat_conn.cursor() as cur:
                    heartbeat_query = """
                    UPDATE status 
                    SET "TIME" = %s, "STATUS" = %s 
                    WHERE "STATUS_TYPE" = 'QUEUE_HEARTBEAT'
                    """
                    # Format time remaining, handling negative values
                    heartbeat_status = f"Next queue population in: {max(timedelta(), time_until_next_queue)}"
                    cur.execute(heartbeat_query, (current_time, heartbeat_status))
                    heartbeat_conn.commit()
        except Exception as e:
            logging.error(f"Error updating queue heartbeat: {e}")
        
        # Check if it's time to populate queue
        if current_time - last_queue_time >= queue_interval:
            try:
                logging.info("Running populate_tasks_table...")
                
                with connect_to_db() as conn:
                    # Create tasks table if not exists
                    create_tasks_table(conn)
                    
                    # Populate tasks and count
                    tasks_added = populate_tasks_table(conn)
                    
                    # Update queue status in database
                    with conn.cursor() as cur:
                        status_query = """
                        UPDATE status 
                        SET "TIME" = %s, "STATUS" = %s 
                        WHERE "STATUS_TYPE" = 'QUEUE_STATUS'
                        """
                        status_message = f"Queue population successful. Tasks added: {tasks_added}"
                        cur.execute(status_query, (current_time, status_message))
                        conn.commit()
                    
                    logging.info(f"Database population completed. Tasks added: {tasks_added}")
                
                # Update last queue population time
                last_queue_time = current_time
            
            except Exception as e:
                # Log error and update status
                logging.error(f"Queue population failed: {e}")
                try:
                    with connect_to_db() as conn:
                        with conn.cursor() as cur:
                            status_query = """
                            UPDATE status 
                            SET "TIME" = %s, "STATUS" = %s 
                            WHERE "STATUS_TYPE" = 'QUEUE_STATUS'
                            """
                            status_message = f"Queue population failed: {str(e)}"
                            cur.execute(status_query, (current_time, status_message))
                            conn.commit()
                except Exception as db_error:
                    logging.error(f"Could not update status in database: {db_error}")
        
        # Sleep to reduce CPU usage and provide consistent heartbeat
        time.sleep(15)  # 15-second heartbeat
