import os
import mlflow
from mlflow.tracking import MlflowClient
import subprocess
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime

# MLflow Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server-service.geoboundaries.svc.cluster.local:5000"
EXPERIMENT_NAME = "Pull from Github"
NEXT_SCRIPT = "collect_data.py"  # Path to your next script

# Database Configuration
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""  # Trust-based auth, no password
DB_PORT = 5432

# Task Directory
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"

# Initialize MLflow client
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
client = MlflowClient()

def connect_to_db():
    """Establishes a connection to the PostGIS database."""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_SERVICE,
            port=DB_PORT
        )
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        sys.exit(1)

def create_tasks_table(conn):
    """Creates the 'Tasks' table if it does not already exist."""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS Tasks (
        ISO VARCHAR(10),
        ADM VARCHAR(10),
        time_added TIMESTAMP,
        time_completed TIMESTAMP,
        runtime INTERVAL,
        filesize BIGINT,
        status VARCHAR(20)
    );
    """
    with conn.cursor() as cur:
        cur.execute(create_table_query)
        conn.commit()
    print("Table 'Tasks' created or verified successfully.")

def populate_tasks_table(conn):
    """Iterates over files in the TASK_DIR and populates the 'Tasks' table."""
    with conn.cursor() as cur:
        for filename in os.listdir(TASK_DIR):
            if filename.endswith(".zip"):
                try:
                    # Parse ISO and ADM from filename
                    iso, adm = filename.split(".zip")[0].split("_")
                    file_path = os.path.join(TASK_DIR, filename)
                    file_size = os.path.getsize(file_path)  # File size in bytes

                    # Check if task already exists
                    select_query = sql.SQL("SELECT COUNT(*) FROM Tasks WHERE ISO = %s AND ADM = %s")
                    cur.execute(select_query, (iso, adm))
                    if cur.fetchone()[0] > 0:
                        print(f"Task for {filename} already exists. Skipping.")
                        continue

                    # Insert task into table
                    insert_query = sql.SQL("""
                        INSERT INTO Tasks (ISO, ADM, time_added, filesize, status)
                        VALUES (%s, %s, %s, %s, %s)
                    """)
                    cur.execute(insert_query, (iso, adm, datetime.now(), file_size, "ready"))
                    print(f"Task for {filename} added successfully.")

                except Exception as e:
                    print(f"Error processing file {filename}: {e}")

        conn.commit()
    print("All tasks have been populated into the 'Tasks' table.")

def get_last_run_status(experiment_name):
    """Fetch the most recent run's status from the specified MLflow experiment."""
    experiment = client.get_experiment_by_name(experiment_name)
    if not experiment:
        print(f"Experiment '{experiment_name}' not found.")
        return None

    # Search for the most recent run
    runs = client.search_runs(experiment_ids=[experiment.experiment_id],
                              order_by=["start_time DESC"],
                              max_results=1)
    if not runs:
        print(f"No runs found for experiment '{experiment_name}'.")
        return None

    # Fetch status metric
    last_run = runs[0]
    status = last_run.data.metrics.get("status")
    print(f"Most recent run ID: {last_run.info.run_id}, Status: {status}")
    return status

def trigger_next_script():
    """Trigger the next script (e.g., data collection)."""
    print(f"Triggering the next script: {NEXT_SCRIPT}")
    result = subprocess.run(["python", NEXT_SCRIPT], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    if result.returncode == 0:
        print("Next script executed successfully.")
    else:
        print("Next script failed.")
        print(result.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # Check the last run's status
    last_status = get_last_run_status(EXPERIMENT_NAME)
    if last_status == 1:  # Success
        print("Previous run succeeded. Proceeding to populate the database...")
        # Connect to the database and populate tasks
        conn = connect_to_db()
        try:
            create_tasks_table(conn)
            populate_tasks_table(conn)
        finally:
            conn.close()
        print("Database population completed.")

        # Trigger the next script
        trigger_next_script()
    else:
        print("Previous run did not succeed. Halting execution.")
        sys.exit(1)
