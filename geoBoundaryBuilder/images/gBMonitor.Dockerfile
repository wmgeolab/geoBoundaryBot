FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    Flask==2.3.3 \
    psycopg2-binary==2.9.7 \
    Werkzeug==2.3.7

# Copy the monitor app with templates
COPY ./gbWeb/monitor/ /app/monitor/

# Copy static web files
COPY ./gbWeb/ /app/web/

# Install development dependencies
RUN pip install --no-cache-dir watchdog

# Set environment variables for development
ENV FLASK_APP=/app/monitor/app.py
ENV FLASK_ENV=development


EXPOSE 5000

# Use the Flask development server with auto-reload
CMD ["flask", "run", "--host=0.0.0.0", "--port=5000", "--reload"]
