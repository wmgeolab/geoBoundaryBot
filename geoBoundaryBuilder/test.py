import os
import subprocess

# Configure Prefect home directory and API URL
os.environ["PREFECT_HOME"] = "/tmp/.prefect"  # Ensure this path is writable
PREFECT_API_URL = "http://prefect-server-service.geoboundaries.svc.cluster.local:4200/api"
os.environ["PREFECT_API_URL"] = PREFECT_API_URL  # Directly set in environment
print(f"Using PREFECT_API_URL: {PREFECT_API_URL}")
subprocess.run(["prefect", "config", "set", f"PREFECT_API_URL={PREFECT_API_URL}"], check=True)


from prefect import flow, task


# Define a task to process numbers
@task
def multiply_by_20(number):
    return number * 20

# Define the main flow
@flow
def process_numbers_flow(numbers):
    results = []
    for number in numbers:
        result = multiply_by_20.submit(number)  # Submit tasks asynchronously
        results.append(result)
    return [r.result() for r in results]  # Gather the results

if __name__ == "__main__":
    # Deploy the flow
    print("Deploying the flow...")
    process_numbers_flow.deploy(
        name="process-numbers-deployment",
        work_pool_name="k8s-gB",
        image="ghcr.io/wmgeolab/gb-base:latest"  # Ensure this matches your Kubernetes work pool
    )
    
    # Run the flow locally for testing
    print("Running the flow locally...")
    numbers = list(range(1, 11))
    results = process_numbers_flow(numbers)
    print("Results:", results)
