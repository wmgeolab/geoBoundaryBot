import os
import mlflow
from mlflow.tracking import MlflowClient
import subprocess
import sys
import psycopg2
from psycopg2 import sql
from datetime import datetime
import time

# Database Configuration
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""  # Trust-based auth, no password
DB_PORT = 5432

# MLflow Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server-service.geoboundaries.svc.cluster.local:5000"
BUILD_TASKS_EXPERIMENT = "Build Tasks"
PREVIOUS_EXPERIMENT = "Pull from Github"

# Task Directory
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"

# Initialize MLflow
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)
mlflow.set_experiment(BUILD_TASKS_EXPERIMENT)
client = MlflowClient()


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
        return conn
    except Exception as e:
        print(f"Database connection failed: {e}")
        mlflow.log_metric("status", 0)  # Log failure status
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
    tasks_added = 0
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
                    tasks_added += 1
                    print(f"Task for {filename} added successfully.")

                except Exception as e:
                    print(f"Error processing file {filename}: {e}")
                    mlflow.log_metric("status", 0)  # Log failure status
        conn.commit()
    return tasks_added


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


if __name__ == "__main__":
    start_time = time.time()

    # Check the last run's status
    last_status = get_last_run_status(PREVIOUS_EXPERIMENT)
    if last_status == 1:  # Success
        print("Previous run succeeded. Proceeding to populate the database...")
        with mlflow.start_run(run_name="Build Tasks Execution"):
            try:
                mlflow.log_param("task_directory", TASK_DIR)
                conn = connect_to_db()
                create_tasks_table(conn)
                tasks_added = populate_tasks_table(conn)
                runtime = time.time() - start_time

                # Log metrics
                mlflow.log_metric("tasks_added", tasks_added)
                mlflow.log_metric("runtime_seconds", runtime)
                mlflow.log_metric("status", 1)  # Success

                print(f"Database population completed. Tasks added: {tasks_added}")
            except Exception as e:
                mlflow.log_metric("status", 0)  # Failure
                print(f"Error during execution: {e}")
                sys.exit(1)
            finally:
                conn.close()
    else:
        print("Previous run did not succeed. Halting execution.")
        sys.exit(1)
