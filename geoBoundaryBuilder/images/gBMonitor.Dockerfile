FROM python:3.9-slim

WORKDIR /app

RUN pip install --no-cache-dir \
    Flask==2.3.3 \
    psycopg2-binary==2.9.7 \
    Werkzeug==2.3.7

# Copy all web files
COPY ./gbWeb/ /app/web/

EXPOSE 5000

CMD ["python", "/app/web/monitor/app.py"]
