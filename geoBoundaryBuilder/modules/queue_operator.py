import os
import subprocess
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime
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


def connect_to_db():
    """Establishes a connection to the PostGIS database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password="",
            host=DB_SERVICE,
            port=DB_PORT
        )
        logging.info("Database connection established.")
        return conn
    except Exception as e:
        logging.error(f"Database connection failed: {e}")
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


if __name__ == "__main__":
    logging.info("Script started.")
    while True:
        current_time = time.localtime()
        if current_time.tm_hour == 3 and current_time.tm_min == 0:
            logging.info("Running populate_tasks_table...")
            with connect_to_db() as conn:
                create_tasks_table(conn)
                tasks_added = populate_tasks_table(conn)
                logging.info(f"Database population completed. Tasks added: {tasks_added}")
            time.sleep(60)  # Sleep for 60 seconds to avoid multiple triggers
        time.sleep(1)  # Check every second
