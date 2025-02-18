import time
import logging
from logging.handlers import TimedRotatingFileHandler
import subprocess
import os
import sys
import psycopg2
from datetime import datetime, timedelta

# Set up logging
log_dir = "/sciclone/geograd/geoBoundaries/logs/git_operator/"
os.makedirs(log_dir, exist_ok=True)
log_file = os.path.join(log_dir, "git_operator.log")

# Configure logging
handler = TimedRotatingFileHandler(log_file, when='midnight', interval=1)
handler.suffix = "%Y-%m-%d.log"
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S %Z',
    handlers=[handler]
)

# Constants
SSH_DIR = "/sciclone/geograd/geoBoundaries/.ssh"
GIT_REPO_DIR = "/sciclone/geograd/geoBoundaries/database/geoBoundaries"
KNOWN_HOSTS = os.path.join(SSH_DIR, "known_hosts")
SSH_KEY = os.path.join(SSH_DIR, "id_ed25519")
HOME_DIR = "/sciclone/geograd/geoBoundaries"

# Database Configuration
DB_SERVICE = "geoboundaries-postgres-service"
DB_NAME = "geoboundaries"
DB_USER = "geoboundaries"
DB_PASSWORD = ""  # Trust-based auth, no password
DB_PORT = 5432

def connect_to_db():
    """Establishes a connection to the PostGIS database with retry mechanism."""
    max_retries = 5
    base_delay = 1  # Initial delay in seconds
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(
                dbname=DB_NAME,
                user=DB_USER,
                password="",
                host=DB_SERVICE,
                port=DB_PORT
            )
            logging.info("Database connection established.")
            return conn
        except Exception as e:
            if attempt < max_retries - 1:
                # Calculate exponential backoff delay
                delay = base_delay * (2 ** attempt)
                logging.warning(f"Database connection attempt {attempt + 1} failed: {e}. "
                                f"Retrying in {delay} seconds...")
                time.sleep(delay)
            else:
                logging.error(f"Database connection failed after {max_retries} attempts: {e}")
                raise

def log_subprocess_output(cmd, result, log_success=True, log_error=True):
    """
    Logs subprocess command output with detailed formatting.
    
    Args:
        cmd (str): The command that was run
        result (subprocess.CompletedProcess): The result of subprocess.run
        log_success (bool): Whether to log successful outputs
        log_error (bool): Whether to log error outputs
    """
    # Log the command
    logging.info(f"Command executed: {cmd}")
    
    # Log stdout if successful and log_success is True
    if result.stdout and log_success:
        logging.info("Command STDOUT:\n" + result.stdout.strip())
    
    # Log stderr only if it contains a real error
    if result.stderr:
        # Filter out known non-error messages
        filtered_stderr = "\n".join([
            line for line in result.stderr.strip().split("\n") 
            if line and 
            not line.startswith(" = [up to date]") and 
            "Already up to date" not in line
        ])
        
        # Log stderr only if there's a real error message after filtering
        if filtered_stderr and (result.returncode != 0 or log_error):
            logging.error("Command STDERR:\n" + filtered_stderr)
    
    return result.returncode == 0

def git_pull():
    """Executes git pull and updates database status with detailed logging."""
    logging.info("Starting git pull and LFS sync process...")
    start_time = datetime.now()
    overall_success = True
    
    try:
        # Immediately update database to show pull is starting
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                start_status_query = """
                UPDATE status 
                SET "TIME" = %s, "STATUS" = %s 
                WHERE "STATUS_TYPE" = 'GIT_PULL'
                """
                cur.execute(start_status_query, (start_time, "Git Pull Started"))
                conn.commit()

        # Set up SSH directory
        os.makedirs(SSH_DIR, exist_ok=True)
        os.chmod(SSH_DIR, 0o700)
        logging.info(f"SSH directory created/verified at: {SSH_DIR}")

        # Add GitHub to known hosts
        subprocess.run(["ssh-keyscan", "-H", "github.com"], stdout=open(KNOWN_HOSTS, "a"), check=True)
        os.chmod(KNOWN_HOSTS, 0o600)
        logging.info(f"Known hosts updated at: {KNOWN_HOSTS}")

        # Prepare git environment
        git_env = {
            "HOME": HOME_DIR,
            "GIT_SSH_COMMAND": f"ssh -i {SSH_KEY} -o UserKnownHostsFile={KNOWN_HOSTS}"
        }

        # Git config
        config_cmd = f"git -C {GIT_REPO_DIR} config --global --add safe.directory {GIT_REPO_DIR}"
        config_result = subprocess.run(
            config_cmd,
            shell=True,
            env=git_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if not log_subprocess_output(config_cmd, config_result):
            overall_success = False

        # Pull latest changes
        pull_cmd = f"git -C {GIT_REPO_DIR} pull --verbose"
        pull_result = subprocess.run(
            pull_cmd,
            shell=True,
            env=git_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        if not log_subprocess_output(pull_cmd, pull_result):
            overall_success = False

        # LFS fetch
        lfs_cmd = f"git -C {GIT_REPO_DIR} lfs fetch --all"
        lfs_result = subprocess.run(
            lfs_cmd,
            shell=True,
            env=git_env,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )
        
        # Check LFS fetch result
        lfs_fetch_success = True
        if lfs_result.returncode != 0:
            # Check if the error is specifically about pointer files
            if "file that should have been a pointer" in lfs_result.stderr:
                logging.warning("LFS pointer file issue detected. Logging error but continuing sync.")
                logging.warning(f"LFS fetch stderr: {lfs_result.stderr}")
            else:
                logging.error(f"LFS fetch failed: {lfs_result.stderr}")
                lfs_fetch_success = False
                overall_success = False

        # Determine overall status
        status_message = "Successful git pull and LFS sync" if overall_success else "Partial or complete git sync failure"
        
        # Calculate and log total runtime
        end_time = datetime.now()
        runtime = end_time - start_time
        logging.info(f"Total git sync runtime: {runtime}")
        
        # Update database status
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                update_query = """
                UPDATE status 
                SET "TIME" = %s, "STATUS" = %s 
                WHERE "STATUS_TYPE" = 'GIT_PULL'
                """
                cur.execute(update_query, (end_time, status_message))
                conn.commit()

    except Exception as e:
        logging.exception(f"Critical error during git sync process: {str(e)}")
        # Attempt to log error to database
        try:
            with connect_to_db() as conn:
                with conn.cursor() as cur:
                    update_query = """
                    UPDATE status 
                    SET "TIME" = %s, "STATUS" = %s 
                    WHERE "STATUS_TYPE" = 'GIT_PULL'
                    """
                    cur.execute(update_query, (datetime.now(), f"Git sync failed: {str(e)}"))
                    conn.commit()
        except Exception as db_error:
            logging.error(f"Could not log error to database: {db_error}")

def check_git_pull_status():
    """Check if git pull is needed based on conditions:
    1. No tasks are currently set to 'ready'
    2. At least 3 hours since last task was processed
    3. At least 12 hours since last pull

    Returns:
        bool: True if a git pull was performed, False otherwise
    """
    try:
        with connect_to_db() as conn:
            with conn.cursor() as cur:
                # Check for ready tasks
                cur.execute('SELECT COUNT(*) FROM tasks WHERE status = \'ready\'')
                ready_count = cur.fetchone()[0]
                if ready_count > 0:
                    logging.info(f"Found {ready_count} ready tasks. Skipping git pull.")
                    # Get last successful pull time for status message
                    cur.execute('SELECT "TIME" FROM status WHERE "STATUS_TYPE" = \'GIT_PULL\'')
                    last_pull = cur.fetchone()
                    last_pull_time = last_pull[0] if last_pull else 'Never'
                    status_msg = f"Git pull skipped due to ongoing tasks; last successful pull was {last_pull_time}"
                    cur.execute(
                        'UPDATE status SET "TIME" = %s, "STATUS" = %s WHERE "STATUS_TYPE" = \'GIT_PULL\'',
                        (datetime.now(), status_msg)
                    )
                    conn.commit()
                    return False

                # Check time since last processed task
                cur.execute("""
                    SELECT MAX(status_time) 
                    FROM tasks 
                    WHERE status = 'COMPLETE'
                """)
                last_task_time = cur.fetchone()[0]
                if last_task_time:
                    time_since_last_task = datetime.now() - last_task_time
                    if time_since_last_task < timedelta(hours=3):
                        logging.info(f"Only {time_since_last_task} since last task completion. Skipping git pull.")
                        # Get last successful pull time for status message
                        cur.execute('SELECT "TIME" FROM status WHERE "STATUS_TYPE" = \'GIT_PULL\'')
                        last_pull = cur.fetchone()
                        last_pull_time = last_pull[0] if last_pull else 'Never'
                        status_msg = f"Git pull skipped due to recent task activity; last successful pull was {last_pull_time}"
                        cur.execute(
                            'UPDATE status SET "TIME" = %s, "STATUS" = %s WHERE "STATUS_TYPE" = \'GIT_PULL\'',
                            (datetime.now(), status_msg)
                        )
                        conn.commit()
                        return False

                # Check time since last git pull
                cur.execute('SELECT "TIME" FROM status WHERE "STATUS_TYPE" = \'GIT_PULL\'')
                result = cur.fetchone()
                
                if result is None:
                    # No record exists, first time running
                    git_pull()
                    return True

                last_pull_time = result[0]
                time_since_last_pull = datetime.now() - last_pull_time

                if time_since_last_pull >= timedelta(hours=12):
                    logging.info(f"Last git pull was {time_since_last_pull} ago. Running git pull.")
                    git_pull()
                    return True
                else:
                    logging.info(f"Last git pull was {time_since_last_pull} ago. No action needed.")
                    status_msg = f"Git pull skipped (too soon); last successful pull was {last_pull_time}"
                    cur.execute(
                        'UPDATE status SET "TIME" = %s, "STATUS" = %s WHERE "STATUS_TYPE" = \'GIT_PULL\'',
                        (datetime.now(), status_msg)
                    )
                    conn.commit()
                    return False
    except Exception as e:
        logging.error(f"Error checking git pull status: {e}")
        return False

if __name__ == "__main__":
    logging.info("Script started.")
    pull_check_interval = timedelta(hours=12)
    
    while True:
        current_time = datetime.now()
        
        # Get last pull time from database
        last_pull_time = None
        try:
            with connect_to_db() as conn:
                with conn.cursor() as cur:
                    cur.execute('SELECT "TIME" FROM status WHERE "STATUS_TYPE" = \'GIT_PULL\'')
                    result = cur.fetchone()
                    if result:
                        last_pull_time = result[0]
        except Exception as e:
            logging.error(f"Error getting last pull time: {e}")
            last_pull_time = current_time  # Default to current time on error
        
        # Perform git pull status check
        check_git_pull_status()
        
        # Calculate time until next pull using database timestamp
        if last_pull_time:
            time_until_next_pull = pull_check_interval - (current_time - last_pull_time)
        else:
            time_until_next_pull = timedelta()
        
        # Update heartbeat in database
        try:
            with connect_to_db() as conn:
                with conn.cursor() as cur:
                    heartbeat_query = """
                    UPDATE status 
                    SET "TIME" = %s, "STATUS" = %s 
                    WHERE "STATUS_TYPE" = 'GIT_HEARTBEAT'
                    """
                    # Format time remaining, handling negative values
                    time_status = f"Next GitHub status check in: {max(timedelta(), time_until_next_pull)}"
                    cur.execute(heartbeat_query, (current_time, time_status))
                    conn.commit()
        except Exception as e:
            logging.error(f"Error updating git heartbeat: {e}")
        
        # Sleep to reduce CPU usage and provide consistent heartbeat
        time.sleep(15)  # 15-second heartbeat
