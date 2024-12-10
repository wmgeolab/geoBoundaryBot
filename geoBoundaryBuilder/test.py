import subprocess
from prefect import flow, task

# Step 1: Set Prefect API URL dynamically
PREFECT_API_URL = "http://prefect-server-service.geoboundaries.svc.cluster.local:4200/api"
subprocess.run(["prefect", "config", "set", f"PREFECT_API_URL={PREFECT_API_URL}"], check=True)

# Step 2: Define a task to process individual numbers
@task
def multiply_by_20(number):
    return number * 20

# Step 3: Define the main flow to process a list of numbers
@flow
def process_numbers_flow(numbers):
    results = []
    for number in numbers:
        result = multiply_by_20.submit(number)  # Submit tasks asynchronously
        results.append(result)
    return [r.result() for r in results]  # Gather the results

# Step 4: Main script to deploy and run the flow
if __name__ == "__main__":
    # Define the list of numbers
    numbers = list(range(1, 11))
    
    # Deploy the flow (if needed)
    print("Deploying the flow...")
    process_numbers_flow.deploy(name="process-numbers-deployment", work_pool_name="k8s-gB")

    # Run the flow locally
    print("Running the flow locally...")
    results = process_numbers_flow(numbers)
    print("Results:", results)
