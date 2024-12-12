import mlflow
from kubernetes import client, config
import time

# Set MLflow tracking URI (replace with your server's URL)
MLFLOW_TRACKING_URI = "http://mlflow-server-service.geoboundaries.svc.cluster.local:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Launch pods on Kubernetes
def launch_k8s_pod(pod_name, param):
    # Kubernetes pod specification
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "geoboundaries"},
        "spec": {
            "containers": [
                {
                    "name": pod_name,
                    "image": "ghcr.io/wmgeolab/gb-base:latest",
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        f"pip install mlflow && "
                        f"python -c 'import mlflow; "
                        f"mlflow.set_tracking_uri(\"{MLFLOW_TRACKING_URI}\"); "
                        f"mlflow.start_run(); "
                        f"mlflow.log_param(\"input\", {param}); "
                        f"output = {param} * 2; "
                        f"mlflow.log_metric(\"output\", output);'"
                    ],
                }
            ],
            "restartPolicy": "Never",
        },
    }

    # Create the pod in the Kubernetes cluster in the "geoboundaries" namespace
    core_v1 = client.CoreV1Api()
    core_v1.create_namespaced_pod(namespace="geoboundaries", body=pod_manifest)
    print(f"Launched Kubernetes Pod: {pod_name}")

# Wait for all Kubernetes pods to complete
def wait_for_pods_completion(pod_names, namespace="geoboundaries"):
    core_v1 = client.CoreV1Api()
    while True:
        completed_pods = 0
        for pod_name in pod_names:
            pod_status = core_v1.read_namespaced_pod_status(name=pod_name, namespace=namespace)
            if pod_status.status.phase in ["Succeeded", "Failed"]:
                completed_pods += 1
        if completed_pods == len(pod_names):
            break
        print(f"Waiting for pods to complete... {completed_pods}/{len(pod_names)} done.")
        time.sleep(5)

# Launch a pod to aggregate results
def launch_aggregation_pod(pod_name, values):
    values_string = ",".join(map(str, values))
    pod_manifest = {
        "apiVersion": "v1",
        "kind": "Pod",
        "metadata": {"name": pod_name, "namespace": "geoboundaries"},
        "spec": {
            "containers": [
                {
                    "name": pod_name,
                    "image": "ghcr.io/wmgeolab/gb-base:latest",
                    "command": ["/bin/sh", "-c"],
                    "args": [
                        f"pip install mlflow && "
                        f"python -c 'import mlflow; "
                        f"mlflow.set_tracking_uri(\"{MLFLOW_TRACKING_URI}\"); "
                        f"values = [{values_string}]; "
                        f"total = sum(values); "
                        f"mlflow.start_run(); "
                        f"mlflow.log_param(\"num_values\", len(values)); "
                        f"mlflow.log_metric(\"total_sum\", total); "
                        f"print(f\"Logged total sum: {total}\");'"
                    ],
                }
            ],
            "restartPolicy": "Never",
        },
    }

    # Create the pod for aggregation in the Kubernetes cluster
    core_v1 = client.CoreV1Api()
    core_v1.create_namespaced_pod(namespace="geoboundaries", body=pod_manifest)
    print(f"Launched Aggregation Pod: {pod_name}")

# Main function
if __name__ == "__main__":
    # Configure Kubernetes client to use your kubeconfig
    kubeconfig_path = "/sciclone/geograd/geoBoundaries/.kube/config"  # Path to your kubeconfig file
    try:
        config.load_kube_config(config_file=kubeconfig_path)
        print(f"Loaded kubeconfig from file: {kubeconfig_path}")
    except Exception as e:
        print(f"Failed to load kubeconfig: {e}")
        raise

    # Step A: Create a list of integers [1:10]
    values = list(range(1, 11))
    pod_names = []

    # Step B: Launch Kubernetes pods for each value
    for value in values:
        pod_name = f"mlflow-pod-{value}"
        pod_names.append(pod_name)
        launch_k8s_pod(pod_name, value)

    # Wait for all pods to complete
    wait_for_pods_completion(pod_names)

    # Step C: Launch aggregation pod
    aggregation_pod_name = "aggregation-pod"
    launch_aggregation_pod(aggregation_pod_name, [value * 2 for value in values])

    # Wait for the aggregation pod to complete
    wait_for_pods_completion([aggregation_pod_name])

    print("All tasks completed.")
