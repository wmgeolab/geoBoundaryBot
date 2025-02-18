from flask import Flask, render_template, jsonify
import psycopg2
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

app = Flask(__name__)

# Database Configuration
DB_SERVICE = os.getenv("DB_SERVICE", "geoboundaries-postgres-service")
DB_NAME = os.getenv("DB_NAME", "geoboundaries")
DB_USER = os.getenv("DB_USER", "geoboundaries")
DB_PASSWORD = os.getenv("DB_PASSWORD", "")
DB_PORT = 5432

def get_db_connection():
    return psycopg2.connect(
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_SERVICE,
        port=DB_PORT
    )

@app.route('/api/worker-grid')
def get_worker_grid():
    """Get status for all ISO/ADM combinations"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get all worker statuses
                cur.execute("""
                    SELECT "STATUS_TYPE", "STATUS", "TIME"
                    FROM status
                    WHERE "STATUS_TYPE" LIKE '%_WORKER'
                """)
                rows = cur.fetchall()
                
                # Process into grid format
                grid_data = []
                for row in rows:
                    # Parse ISO and ADM from STATUS_TYPE (format: ISO_ADM#_WORKER)
                    parts = row[0].split('_')
                    if len(parts) >= 3:
                        iso = parts[0]
                        adm = parts[1].replace('ADM', '')
                        status = row[1]
                        timestamp = row[2].replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/New_York')).isoformat() if row[2] else None
                        
                        grid_data.append({
                            'iso': iso,
                            'adm': adm,
                            'status': status,
                            'time': timestamp
                        })
                
                return jsonify({'grid_data': grid_data})
    except Exception as e:
        logging.error(f"Error getting worker grid data: {e}")
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats')
def get_stats():
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                # Get number of ready tasks
                cur.execute("SELECT COUNT(*) FROM Tasks WHERE status = 'ready'")
                ready_count = cur.fetchone()[0]
                logging.info(f"Ready tasks count: {ready_count}")

                # Get number of tasks processed in last 24 hours
                cur.execute("""
                    SELECT COUNT(*) FROM Tasks 
                    WHERE status_time >= NOW() - INTERVAL '24 hours'
                """)
                processed_24h = cur.fetchone()[0]
                logging.info(f"Processed in 24h: {processed_24h}")

                # Get oldest ready task
                cur.execute("""
                    SELECT time_added FROM Tasks 
                    WHERE status = 'ready' 
                    ORDER BY time_added ASC 
                    LIMIT 1
                """)
                oldest_ready = cur.fetchone()
                logging.info(f"Oldest ready task query result: {oldest_ready}")
                oldest_ready_time = None
                if oldest_ready and oldest_ready[0]:
                    # Convert from UTC to EST
                    oldest_ready_time = oldest_ready[0].replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/New_York')).isoformat()
                logging.info(f"Oldest ready task time: {oldest_ready_time}")

                # Get status information
                cur.execute("""
                    WITH git_status AS (
                        SELECT 'GIT' as component,
                               "STATUS" as status_message,
                               "TIME" as status_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'GIT_PULL'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    ),
                    git_heartbeat AS (
                        SELECT "STATUS" as heartbeat_message,
                               "TIME" as heartbeat_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'GIT_HEARTBEAT'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    ),
                    queue_status AS (
                        SELECT 'QUEUE' as component,
                               "STATUS" as status_message,
                               "TIME" as status_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'QUEUE_STATUS'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    ),
                    queue_heartbeat AS (
                        SELECT "STATUS" as heartbeat_message,
                               "TIME" as heartbeat_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'QUEUE_HEARTBEAT'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    ),
                    worker_status AS (
                        SELECT 'WORKER' as component,
                               "STATUS" as status_message,
                               "TIME" as status_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'WORKER_STATUS'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    ),
                    worker_heartbeat AS (
                        SELECT "STATUS" as heartbeat_message,
                               "TIME" as heartbeat_time
                        FROM status
                        WHERE "STATUS_TYPE" = 'WORKER_OP_HEARTBEAT'
                        ORDER BY "TIME" DESC
                        LIMIT 1
                    )
                    SELECT 
                        gs.component,
                        gs.status_message,
                        gs.status_time,
                        gh.heartbeat_message,
                        gh.heartbeat_time
                    FROM git_status gs
                    LEFT JOIN git_heartbeat gh ON true
                    UNION ALL
                    SELECT 
                        qs.component,
                        qs.status_message,
                        qs.status_time,
                        qh.heartbeat_message,
                        qh.heartbeat_time
                    FROM queue_status qs
                    LEFT JOIN queue_heartbeat qh ON true
                    UNION ALL
                    SELECT 
                        ws.component,
                        ws.status_message,
                        ws.status_time,
                        wh.heartbeat_message,
                        wh.heartbeat_time
                    FROM worker_status ws
                    LEFT JOIN worker_heartbeat wh ON true
                    ORDER BY component
                """)
                rows = cur.fetchall()
                logging.info(f"Status rows: {rows}")
                
                status_info = []
                for row in rows:
                    # Convert times from UTC to EST
                    status_time = row[2].replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/New_York')) if row[2] else None
                    heartbeat_time = row[4].replace(tzinfo=ZoneInfo('UTC')).astimezone(ZoneInfo('America/New_York')) if row[4] else None
                    
                    status_info.append({
                        'type': row[0],
                        'status_message': row[1],
                        'status_time': status_time.isoformat() if status_time else None,
                        'heartbeat_message': row[3],
                        'heartbeat_time': heartbeat_time.isoformat() if heartbeat_time else None
                    })
                logging.info(f"Processed status info: {status_info}")

                response_data = {
                    'ready_tasks': ready_count,
                    'processed_24h': processed_24h,
                    'oldest_ready': oldest_ready_time,
                    'status_info': status_info
                }
                logging.info(f"Sending response: {response_data}")
                return jsonify(response_data)
    except Exception as e:
        logging.error(f"Error in get_stats: {str(e)}")
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
