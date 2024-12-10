import os
import subprocess

# Step 1: Configure Prefect home directory and API URL
os.environ["PREFECT_HOME"] = "/tmp/.prefect"  # Ensure this path is writable
PREFECT_API_URL = "http://prefect-server-service.geoboundaries.svc.cluster.local:4200/api"
os.environ["PREFECT_API_URL"] = PREFECT_API_URL  # Directly set in environment
print(f"Using PREFECT_API_URL: {PREFECT_API_URL}")  # Debug output
subprocess.run(["prefect", "config", "set", f"PREFECT_API_URL={PREFECT_API_URL}"], check=True)

from prefect import flow
from prefect.context import get_run_context

@flow
def my_flow():
    # Your flow logic here
    print("Running my flow on Kubernetes")

if __name__ == "__main__":
    # Get the current script path
    script_path = __file__
    
    # Define dynamic parameters
    image = "ghcr.io/wmgeolab/gb-base:latest"
    
    # Deploy the flow with dynamic configurations
    deployment = my_flow.deploy(
        name="dynamic-k8s-flow",
        work_pool_name="k8s-gB",
        image=image,
        job_variables={
            "env": {"EXTRA_PIP_PACKAGES": "your-required-packages"},
            "image_pull_policy": "Always",
            "command": [
                "bash",
                "-c",
                f"pip install -r requirements.txt && python {script_path}"
            ]
        }
    )

    # Optionally, run the deployment immediately
    deployment.run()