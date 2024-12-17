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
MAX_RUNNING_PODS = 20  # Limit the number of active worker pods
TASK_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries/sourceData/gbOpen"

# Connect to the database
def connect_to_db():
    return psycopg2.connect(
        dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_SERVICE, port=DB_PORT
    )

# Get count of running worker pods
def get_running_pods_count():
    config.load_incluster_config()
    k8s_client = client.CoreV1Api()
    pods = k8s_client.list_namespaced_pod(namespace=NAMESPACE, label_selector="app=worker")
    return len([pod for pod in pods.items if pod.status.phase in ["Pending", "Running"]])

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
                    SET status = 'working' 
                    WHERE ISO = %s AND ADM = %s AND status = 'ready';
                """, (iso, adm))
                conn.commit()
                print(f"Task {iso}_{adm} marked as 'working'.")
    except Exception as e:
        print(f"Error while fetching and updating tasks: {e}")
    finally:
        conn.close()
    return task

# Create a Worker Pod
def create_worker_pod(iso, adm, filesize):
    config.load_incluster_config()
    k8s_client = client.CoreV1Api()

    memory_request = max(128, filesize * 10)  # Memory request in Mi

    pod_manifest = client.V1Pod(
        metadata=client.V1ObjectMeta(
            name=f"worker-{iso}-{adm}".lower(), 
            namespace=NAMESPACE,
            labels={"app": "worker"}
        ),
        spec=client.V1PodSpec(
            restart_policy="Never",
            containers=[
                client.V1Container(
                    name="worker-container",
                    image="python:3.11-slim",
                    command=["/bin/bash", "-c"],
                    args=[
                        f"pip install zipfile36 && python /scripts/worker_script.py {iso} {adm}"
                    ],
                    env=[
                        client.V1EnvVar(name="DB_SERVICE", value=DB_SERVICE),
                        client.V1EnvVar(name="DB_NAME", value=DB_NAME),
                        client.V1EnvVar(name="DB_USER", value=DB_USER),
                        client.V1EnvVar(name="DB_PASSWORD", value=DB_PASSWORD),
                    ],
                    volume_mounts=[
                        client.V1VolumeMount(
                            mount_path="/scripts",
                            name="script-volume"
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
                    name="script-volume",
                    nfs=client.V1NFSVolumeSource(
                        server="128.239.59.144",
                        path="/sciclone/geograd/geoBoundaries/geoBoundaryBot/geoBoundaryBuilder"
                    )
                )
            ],
        ),
    )

    k8s_client.create_namespaced_pod(namespace=NAMESPACE, body=pod_manifest)
    print(f"Created worker pod for task: ISO={iso}, ADM={adm}, Memory={memory_request}Mi")

# Main Loop
def main():
    print("Starting Task Controller...")
    while True:
        try:
            running_pods = get_running_pods_count()
            print(f"Currently running pods: {running_pods}/{MAX_RUNNING_PODS}")

            if running_pods < MAX_RUNNING_PODS:
                task = get_and_mark_ready_task()
                if task:
                    iso, adm, filesize = task
                    print(f"Launching worker pod for task: ISO={iso}, ADM={adm}")
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
