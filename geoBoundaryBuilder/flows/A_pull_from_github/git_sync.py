import mlflow
import subprocess
import os
import sys

# MLflow Tracking Configuration
MLFLOW_TRACKING_URI = "http://mlflow-server-service.geoboundaries.svc.cluster.local:5000"
mlflow.set_tracking_uri(MLFLOW_TRACKING_URI)

# Set Experiment Name
EXPERIMENT_NAME = "Pull from Github"
mlflow.set_experiment(EXPERIMENT_NAME)

# Constants
SSH_DIR = "/sciclone/geograd/geoBoundaries/.ssh"
GIT_REPO_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries"
KNOWN_HOSTS = os.path.join(SSH_DIR, "known_hosts")
SSH_KEY = os.path.join(SSH_DIR, "id_ed25519")
HOME_DIR = "/sciclone/geograd/geoBoundaries"

def git_pull():
    """Executes git pull and logs results to MLflow."""
    with mlflow.start_run():
        mlflow.log_param("operation", "git pull")
        try:
            # Set up SSH directory
            os.makedirs(SSH_DIR, exist_ok=True)
            os.chmod(SSH_DIR, 0o700)
            subprocess.run(["ssh-keyscan", "-H", "github.com"], stdout=open(KNOWN_HOSTS, "a"), check=True)
            os.chmod(KNOWN_HOSTS, 0o600)

            # Inline git config and git pull
            print("Starting git pull...")
            git_command = (
                f"git -C {GIT_REPO_DIR} config --global --add safe.directory {GIT_REPO_DIR} && "
                f"git -C {GIT_REPO_DIR} pull"
            )
            result = subprocess.run(
                git_command,
                env={
                    "HOME": HOME_DIR,
                    "GIT_SSH_COMMAND": f"ssh -i {SSH_KEY} -o UserKnownHostsFile={KNOWN_HOSTS}"
                },
                shell=True,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
            )

            if result.returncode == 0:
                print("Git pull succeeded.")
                mlflow.log_metric("status", 1)
                mlflow.log_param("git_stdout", result.stdout)
            else:
                print("Git pull failed.", file=sys.stderr)
                mlflow.log_metric("status", 0)
                mlflow.log_param("git_stderr", result.stderr)
                mlflow.log_param("git_stdout", result.stdout)
                sys.exit(1)
        except Exception as e:
            print(f"Error occurred: {e}", file=sys.stderr)
            mlflow.log_metric("status", 0)
            mlflow.log_param("error", str(e))
            sys.exit(1)

if __name__ == "__main__":
    git_pull()
