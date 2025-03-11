FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    Flask==2.3.3 \
    psycopg2-binary==2.9.7 \
    Werkzeug==2.3.7

# Copy the monitor app
COPY ./gbWeb/monitor/ /app/monitor/

# Copy the static files
COPY ./gbWeb/ /app/static/

EXPOSE 5000

CMD ["python", "/app/monitor/app.py"]
