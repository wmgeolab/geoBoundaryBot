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

                # Get number of tasks processed in last 24 hours
                cur.execute("""
                    SELECT COUNT(*) FROM Tasks 
                    WHERE status_time >= NOW() - INTERVAL '24 hours'
                """)
                processed_24h = cur.fetchone()[0]

                # Get oldest ready task
                cur.execute("""
                    SELECT time_added FROM Tasks 
                    WHERE status = 'ready' 
                    ORDER BY time_added ASC 
                    LIMIT 1
                """)
                oldest_ready = cur.fetchone()
                oldest_ready_time = oldest_ready[0].isoformat() if oldest_ready else None

                return jsonify({
                    'ready_tasks': ready_count,
                    'processed_24h': processed_24h,
                    'oldest_ready': oldest_ready_time
                })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/')
def index():
    return render_template('index.html')

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
