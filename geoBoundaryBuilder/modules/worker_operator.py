import logging
from logging.handlers import TimedRotatingFileHandler
import os
import psycopg2
import time
from kubernetes import client, config
from datetime import datetime, timedelta

# Logging Configuration
log_dir = "/sciclone/geograd/geoBoundaries/logs/worker_operator"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "worker_operator.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()  # Also log to console
    ]
)

# Rotate log files daily
file_handler = TimedRotatingFileHandler(
    log_file, 
    when='midnight', 
    interval=1, 
    backupCount=30  # Keep logs for 30 days
)
file_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logging.getLogger().addHandler(file_handler)

# Database Configuration
DB_SERVICE = os.getenv("DB_SERVICE", "geoboundaries-postgres-service")
DB_NAME = os.getenv("DB_NAME", "geoboundaries")
DB_USER = os.getenv("DB_USER", "geoboundaries")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = 5432

# Pod Configuration
NAMESPACE = "geoboundaries"
MAX_RUNNING_PODS = 1  # Limit the number of active worker pods
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"
MAX_FAILED_PODS = 3  # Maximum failed pods before stopping task controller

# Counter for failed pods
failed_pod_count = 0

# Load Kubernetes Configuration
def load_kubernetes_config():
    try:
        config_file = "/sciclone/geograd/geoBoundaries/.kube/config"
        if os.path.exists(config_file):
            config.load_kube_config(config_file=config_file)
            logging.info(f"Kubernetes configuration loaded from: {config_file}")
        else:
            raise FileNotFoundError(f"Kubeconfig file not found at: {config_file}")
    except Exception as e:
        logging.error(f"Error loading Kubernetes configuration: {e}")
        raise

# Connect to the database
def connect_to_db():
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_SERVICE, port=DB_PORT
        )
        logging.debug("Database connection established successfully.")
        return conn
    except Exception as e:
        logging.error(f"Failed to connect to database: {e}")
        raise

# Update task status, status detail, and status time in the database
def update_task_status(iso, adm, status, detail=None):
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE Tasks 
                    SET status = %s, status_detail = %s, status_time = NOW() 
                    WHERE ISO = %s AND ADM = %s;
                """, (status, detail, iso.upper(), adm.upper()))
                conn.commit()
                logging.info(f"Task {iso.upper()}_{adm.upper()} marked as '{status}' with details: {detail}.")
    except Exception as e:
        logging.error(f"Error updating task status for {iso.upper()}_{adm.upper()}: {e}")

# Get count of running worker pods
def get_running_pods_count():
    try:
        k8s_client = client.CoreV1Api()
        pods = k8s_client.list_namespaced_pod(namespace=NAMESPACE, label_selector="app=worker")
        running_pods = len([pod for pod in pods.items if pod.status.phase in ["Pending", "Running"]])
        logging.debug(f"Running pods count: {running_pods}")
        return running_pods
    except Exception as e:
        logging.error(f"Error getting running pods count: {e}")
        return 0

# Monitor worker pod status and update task status if it fails
def monitor_worker_pods():
    global failed_pod_count
    try:
        k8s_client = client.CoreV1Api()
        pods = k8s_client.list_namespaced_pod(namespace=NAMESPACE, label_selector="app=worker")

        for pod in pods.items:
            pod_name = pod.metadata.name
            pod_status = pod.status.phase
            completion_time = None

            # Get completion time for completed pods
            if pod_status == "Succeeded" and pod.status.container_statuses:
                container_state = pod.status.container_statuses[0].state
                if hasattr(container_state, 'terminated') and container_state.terminated:
                    completion_time = container_state.terminated.finished_at

            # Handle failed pods
            if pod_status == "Failed":
                # Extract task identifiers from the pod name
                _, iso, adm = pod_name.split("-")
                try:
                    # Fetch pod logs
                    log = k8s_client.read_namespaced_pod_log(name=pod_name, namespace=NAMESPACE)
                    logging.warning(f"Worker pod {pod_name} failed. Updating task {iso.upper()}_{adm.upper()} to 'ERROR'.")
                    update_task_status(iso, adm, "ERROR", detail=log)

                    # Increment failed pod count
                    failed_pod_count += 1
                    logging.info(f"Failed pod count: {failed_pod_count}")

                    # Delete the failed pod
                    k8s_client.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)
                    logging.info(f"Deleted failed pod {pod_name}.")
                except Exception as e:
                    logging.error(f"Error fetching logs or deleting pod {pod_name}: {e}")
            
            # Handle completed pods
            elif pod_status == "Succeeded" and completion_time:
                # Check if pod has been completed for more than 5 minutes
                if datetime.now(completion_time.tzinfo) - completion_time > timedelta(minutes=5):
                    try:
                        k8s_client.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)
                        logging.info(f"Deleted completed pod {pod_name} after 5 minutes.")
                    except Exception as e:
                        logging.error(f"Error deleting completed pod {pod_name}: {e}")

        if failed_pod_count >= MAX_FAILED_PODS:
            logging.critical(f"Maximum failed pods reached ({failed_pod_count}). Stopping task controller.")
            exit(1)
    except Exception as e:
        logging.error(f"Error in monitor_worker_pods: {e}")

# Get a task with status 'ready' and mark it as 'working'
def get_and_mark_ready_task():
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                # Fetch one "ready" task
                cur.execute("""
                    SELECT taskid, ISO, ADM, filesize FROM Tasks 
                    WHERE status = 'ready' 
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED;
                """)
                task = cur.fetchone()

                if task:
                    # Mark the task as 'working'
                    taskid, iso, adm, filesize = task
                    cur.execute("""
                        UPDATE Tasks 
                        SET status = 'working', status_time = NOW() 
                        WHERE taskid = %s;
                    """, (taskid,))
                    conn.commit()
                    logging.info(f"Task {iso.upper()}_{adm.upper()} marked as 'working'.")
                    return task
                else:
                    logging.info("No ready tasks found.")
                    return None
    except Exception as e:
        logging.error(f"Error while fetching and updating tasks: {e}")
        return None

# Create a Worker Pod
def create_worker_pod(iso, adm, filesize, taskid):
    try:
        k8s_client = client.CoreV1Api()

        memory_request = max(16000, (filesize / 1048576) * 10)  # Memory request in Mi

        pod_manifest = client.V1Pod(
            metadata=client.V1ObjectMeta(
                name=f"worker-{iso.lower()}-{adm.lower()}".lower(),
                namespace=NAMESPACE,
                labels={"app": "worker"}
            ),
            spec=client.V1PodSpec(
                restart_policy="Never",
                security_context=client.V1PodSecurityContext(
                    run_as_user=71032,
                    run_as_group=9915
                ),
                containers=[
                    client.V1Container(
                        name="worker-container",
                        image="ghcr.io/wmgeolab/gb-base:latest",
                        command=["/bin/bash", "-c"],
                        args=[
                            f"python /sciclone/geograd/geoBoundaries/geoBoundaryBot/geoBoundaryBuilder/modules/worker_script.py {iso.upper()} {adm.upper()} {taskid}"
                        ],
                        env=[
                            client.V1EnvVar(name="DB_SERVICE", value=DB_SERVICE),
                            client.V1EnvVar(name="DB_NAME", value=DB_NAME),
                            client.V1EnvVar(name="DB_USER", value=DB_USER),
                            client.V1EnvVar(name="DB_PASSWORD", value=DB_PASSWORD),
                        ],
                        volume_mounts=[
                            client.V1VolumeMount(
                                mount_path="/sciclone",
                                name="nfs-mount"
                            )
                        ],
                        resources=client.V1ResourceRequirements(
                            requests={"memory": f"{memory_request}Mi"},
                            limits={"memory": f"{memory_request}Mi"},
                        ),
                    )
                ],
                volumes=[
                    client.V1Volume(
                        name="nfs-mount",
                        nfs=client.V1NFSVolumeSource(
                            server="128.239.59.144",
                            path="/sciclone"
                        )
                    )
                ],
            ),
        )

        k8s_client.create_namespaced_pod(namespace=NAMESPACE, body=pod_manifest)
        logging.info(f"Created worker pod for task: ISO={iso.upper()}, ADM={adm.upper()}, Memory={memory_request}Mi")
    except Exception as e:
        logging.error(f"Failed to create worker pod for task: ISO={iso.upper()}, ADM={adm.upper()}. Error: {e}")
        update_task_status(iso, adm, "ERROR", detail=str(e))

# Main Loop
def main():
    logging.info("Starting Task Controller...")
    load_kubernetes_config()  # Load the kubeconfig before proceeding
    
    # Track time for heartbeat and status updates
    last_heartbeat_time = datetime.now()
    last_status_update_time = datetime.now()
    
    while True:
        try:
            current_time = datetime.now()
            
            # Update worker operator heartbeat every 15 seconds
            if (current_time - last_heartbeat_time).total_seconds() >= 15:
                try:
                    with connect_to_db() as heartbeat_conn:
                        with heartbeat_conn.cursor() as cur:
                            heartbeat_query = """
                            UPDATE status 
                            SET "TIME" = %s, "STATUS" = %s 
                            WHERE "STATUS_TYPE" = 'WORKER_OP_HEARTBEAT'
                            """
                            # Calculate time to next status check
                            time_to_next_status = 60 - (current_time - last_status_update_time).total_seconds()
                            heartbeat_status = f"Next worker status update in: {max(0, time_to_next_status):.2f} seconds"
                            cur.execute(heartbeat_query, (current_time, heartbeat_status))
                            heartbeat_conn.commit()
                    
                    last_heartbeat_time = current_time
                except Exception as e:
                    logging.error(f"Error updating worker operator heartbeat: {e}")
            
            # Update worker status every minute
            if (current_time - last_status_update_time).total_seconds() >= 60:
                try:
                    with connect_to_db() as status_conn:
                        with status_conn.cursor() as cur:
                            # Count successful and failed tasks in last 24 hours
                            status_query = """
                            WITH task_counts AS (
                                SELECT 
                                    COUNT(*) FILTER (WHERE status = 'completed') as successful_tasks,
                                    COUNT(*) FILTER (WHERE status = 'ERROR') as failed_tasks
                                FROM Tasks
                                WHERE status_time >= %s
                            )
                            UPDATE status 
                            SET "TIME" = %s, 
                                "STATUS" = %s 
                            WHERE "STATUS_TYPE" = 'WORKER_STATUS'
                            """
                            # 24 hours ago
                            twentyfour_hours_ago = current_time - timedelta(hours=24)
                            
                            # Execute count query
                            cur.execute("""
                                SELECT 
                                    COUNT(*) FILTER (WHERE status = 'COMPLETE') as successful_tasks,
                                    COUNT(*) FILTER (WHERE status = 'ERROR') as failed_tasks
                                FROM Tasks
                                WHERE status_time >= %s
                            """, (twentyfour_hours_ago,))
                            successful_tasks, failed_tasks = cur.fetchone()
                            
                            # Prepare status message
                            status_message = (
                                f"Tasks in last 24 hours - "
                                f"Successful: {successful_tasks}, "
                                f"Failed: {failed_tasks}"
                            )
                            
                            # Update status
                            cur.execute(status_query, (
                                twentyfour_hours_ago, 
                                current_time, 
                                status_message
                            ))
                            status_conn.commit()
                    
                    last_status_update_time = current_time
                    logging.info(status_message)
                except Exception as e:
                    logging.error(f"Error updating worker status: {e}")
            
            # Existing worker pod monitoring logic
            monitor_worker_pods()  # Monitor running worker pods
            running_pods = get_running_pods_count()
            logging.info(f"Currently running pods: {running_pods}/{MAX_RUNNING_PODS}")

            if running_pods < MAX_RUNNING_PODS:
                # Get a ready task
                task = get_and_mark_ready_task()
                
                if task:
                    taskid, iso, adm, filesize = task
                    create_worker_pod(iso, adm, filesize, taskid)
            
            # Sleep to reduce CPU usage
            time.sleep(15)
        
        except Exception as e:
            logging.error(f"Error in main loop: {e}")
            time.sleep(30)  # Wait before retrying to prevent rapid error loops

if __name__ == "__main__":
    main()
