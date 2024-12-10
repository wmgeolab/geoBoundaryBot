import os
from prefect import flow, task
from prefect.deployments import Deployment
import subprocess

# Step 1: Set Prefect API URL
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

# Step 4: Main script to run locally or deploy
if __name__ == "__main__":
    # Define the list of numbers
    numbers = list(range(1, 11))
    
    # Create a deployment for the flow
    deployment = Deployment.build_from_flow(
        flow=process_numbers_flow,
        name="process-numbers-deployment",
    )
    deployment.apply()

    # Run the flow directly
    print("Running the flow locally...")
    results = process_numbers_flow(numbers)
    print("Results:", results)
