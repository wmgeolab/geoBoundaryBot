import os
import subprocess
from prefect import flow
from prefect_kubernetes import KubernetesJob  # Use the correct import now that the package is installed

# Step 1: Configure Prefect home directory and API URL
os.environ["PREFECT_HOME"] = "/tmp/.prefect"  # Ensure this path is writable
PREFECT_API_URL = "http://prefect-server-service.geoboundaries.svc.cluster.local:4200/api"
os.environ["PREFECT_API_URL"] = PREFECT_API_URL  # Directly set in environment
print(f"Using PREFECT_API_URL: {PREFECT_API_URL}")  # Debug output
subprocess.run(["prefect", "config", "set", f"PREFECT_API_URL={PREFECT_API_URL}"], check=True)

# Step 2: Define a simple Prefect flow
@flow
def simple_flow():
    print("Hello, Prefect!")
    return "Flow Completed"

# Step 3: Configure and deploy the flow to Kubernetes
if __name__ == "__main__":
    # Define dynamic job variables (e.g., for Kubernetes deployment)
    job_variables = {
        "image_pull_policy": "Always",
        "env": {"EXTRA_PIP_PACKAGES": "prefect kubernetes"}
    }

    # Create Kubernetes job for Prefect deployment
    k8s_infrastructure = KubernetesJob(
        image="python:3.11-slim",  # Just a placeholder; no need for GitHub or image build
        job_variables=job_variables,
        image_pull_policy="Always",
    )

    # Deploy the flow using the created infrastructure
    deployment = simple_flow.deploy(
        name="simple-flow-k8s-deployment",
        work_pool_name="k8s-gB",  # Use the existing work pool from Prefect
        infrastructure=k8s_infrastructure,
    )

    # Optionally, run the deployment immediately
    deployment.run()
