from flask import Flask, render_template, jsonify
import psycopg2
from datetime import datetime, timedelta
import os

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
                oldest_ready_time = oldest_ready[0].isoformat() if oldest_ready else None
                logging.info(f"Oldest ready task time: {oldest_ready_time}")

                # Get status information
                cur.execute("""
                    SELECT status_type, status, last_updated, heartbeat
                    FROM status
                    ORDER BY status_type
                """)
                rows = cur.fetchall()
                logging.info(f"Status rows: {rows}")
                
                status_info = []
                for row in rows:
                    status_info.append({
                        'type': row[0],
                        'status': row[1],
                        'last_updated': row[2].isoformat() if row[2] else None,
                        'heartbeat': row[3].isoformat() if row[3] else None
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
