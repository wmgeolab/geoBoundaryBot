import os
import psycopg2
import time
from kubernetes import client, config

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
            print(f"Kubernetes configuration loaded from: {config_file}")
        else:
            raise FileNotFoundError(f"Kubeconfig file not found at: {config_file}")
    except Exception as e:
        print(f"Error loading Kubernetes configuration: {e}")
        raise

# Connect to the database
def connect_to_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_SERVICE, port=DB_PORT
    )

# Update task status, status detail, and status time in the database
def update_task_status(iso, adm, status, detail=None):
    conn = connect_to_db()
    try:
        with conn.cursor() as cur:
            cur.execute("""
                UPDATE Tasks 
                SET status = %s, status_detail = %s, status_time = NOW() 
                WHERE ISO = %s AND ADM = %s;
            """, (status, detail, iso.upper(), adm.upper()))
            conn.commit()
            print(f"Task {iso.upper()}_{adm.upper()} marked as '{status}' with details: {detail}.")
    except Exception as e:
        print(f"Error updating task status: {e}")
    finally:
        conn.close()

# Get count of running worker pods
def get_running_pods_count():
    k8s_client = client.CoreV1Api()
    pods = k8s_client.list_namespaced_pod(namespace=NAMESPACE, label_selector="app=worker")
    return len([pod for pod in pods.items if pod.status.phase in ["Pending", "Running"]])

# Monitor worker pod status and update task status if it fails
def monitor_worker_pods():
    global failed_pod_count
    k8s_client = client.CoreV1Api()
    pods = k8s_client.list_namespaced_pod(namespace=NAMESPACE, label_selector="app=worker")

    for pod in pods.items:
        pod_name = pod.metadata.name
        pod_status = pod.status.phase

        # If pod failed, update the corresponding task to "ERROR"
        if pod_status == "Failed":
            # Extract task identifiers from the pod name
            _, iso, adm = pod_name.split("-")
            try:
                # Fetch pod logs
                log = k8s_client.read_namespaced_pod_log(name=pod_name, namespace=NAMESPACE)
                print(f"Worker pod {pod_name} failed. Updating task {iso.upper()}_{adm.upper()} to 'ERROR'.")
                update_task_status(iso, adm, "ERROR", detail=log)

                # Increment failed pod count
                failed_pod_count += 1

                # Delete the failed pod
                k8s_client.delete_namespaced_pod(name=pod_name, namespace=NAMESPACE)
                print(f"Deleted failed pod {pod_name}.")
            except Exception as e:
                print(f"Error fetching logs or deleting pod {pod_name}: {e}")

    if failed_pod_count >= MAX_FAILED_PODS:
        print(f"Maximum failed pods reached ({failed_pod_count}). Stopping task controller.")
        exit(1)

# Get a task with status 'ready' and mark it as 'working'
def get_and_mark_ready_task():
    conn = connect_to_db()
    task = None
    try:
        with conn.cursor() as cur:
            # Fetch one "ready" task
            cur.execute("""
                SELECT ISO, ADM, filesize FROM Tasks 
                WHERE status = 'ready' 
                LIMIT 1
                FOR UPDATE SKIP LOCKED;
            """)
            task = cur.fetchone()

            if task:
                # Mark the task as 'working'
                iso, adm, filesize = task
                cur.execute("""
                    UPDATE Tasks 
                    SET status = 'working', status_time = NOW() 
                    WHERE ISO = %s AND ADM = %s AND status = 'ready';
                """, (iso.upper(), adm.upper()))
                conn.commit()
                print(f"Task {iso.upper()}_{adm.upper()} marked as 'working'.")
    except Exception as e:
        print(f"Error while fetching and updating tasks: {e}")
    finally:
        conn.close()
    return task

# Create a Worker Pod
def create_worker_pod(iso, adm, filesize):
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
                        f"python /sciclone/geograd/geoBoundaries/geoBoundaryBot/geoBoundaryBuilder/worker_script.py {iso.upper()} {adm.upper()}"
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

    try:
        k8s_client.create_namespaced_pod(namespace=NAMESPACE, body=pod_manifest)
        print(f"Created worker pod for task: ISO={iso.upper()}, ADM={adm.upper()}, Memory={memory_request}Mi")
    except Exception as e:
        print(f"Failed to create worker pod for task: ISO={iso.upper()}, ADM={adm.upper()}. Error: {e}")
        update_task_status(iso, adm, "ERROR", detail=str(e))

# Main Loop
def main():
    print("Starting Task Controller...")
    load_kubernetes_config()  # Load the kubeconfig before proceeding
    while True:
        try:
            monitor_worker_pods()  # Monitor running worker pods
            running_pods = get_running_pods_count()
            print(f"Currently running pods: {running_pods}/{MAX_RUNNING_PODS}")

            if running_pods < MAX_RUNNING_PODS:
                task = get_and_mark_ready_task()
                if task:
                    iso, adm, filesize = task
                    print(f"Launching worker pod for task: ISO={iso.upper()}, ADM={adm.upper()}")
                    create_worker_pod(iso, adm, filesize)
                else:
                    print("No tasks with status 'ready'. Sleeping...")
            else:
                print(f"Pod limit reached ({MAX_RUNNING_PODS}). Waiting for workers to finish...")

        except Exception as e:
            print(f"Error in main loop: {e}")

        time.sleep(10)  # Sleep for 10 seconds before checking again

if __name__ == "__main__":
    main()
