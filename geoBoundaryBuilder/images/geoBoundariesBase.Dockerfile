# Start with a statically versioned Alpine base image
FROM alpine:3.18

# Set environment variables to prevent interactive prompts and ensure consistent Python behavior
ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install required system dependencies
RUN apk add --no-cache \
    python3=3.11.5-r0 \
    py3-pip=23.1.2-r0 \
    gcc=12.3.1_git20230527-r0 \
    musl-dev=1.2.4-r1 \
    libffi-dev=3.4.4-r0 \
    openssl-dev=3.1.2-r0 \
    g++=12.3.1_git20230527-r0 \
    make=4.3-r1 \
    git=2.40.1-r0 \
    git-lfs=3.4.0-r0 \
    gdal=3.7.1-r0 \
    geos-dev=3.11.1-r0 \
    proj-dev=9.2.0-r0 \
    jpeg-dev=9e-r0 \
    zlib-dev=1.2.13-r1 \
    cairo-dev=1.17.6-r0 \
    py3-cffi=1.15.1-r3 \
    py3-psycopg2=2.9.6-r0

# Install Python packages
RUN pip install --upgrade pip \
    && pip install \
    prefect==2.2.0 \
    kubernetes==25.3.0 \
    geopandas==0.13.2 \
    shapely==2.0.1 \
    matplotlib==3.7.2 \
    pandas==2.1.1 \
    hashlib==20081119 \
    jsonschema==4.19.0 \
    zipfile36==0.1.3

# Install the Prefect Kubernetes package for Kubernetes-specific integrations
RUN pip install prefect-kubernetes

# Set up git-lfs
RUN git lfs install

# Create a directory for Prefect worker configuration (optional)
RUN mkdir -p /etc/prefect

# Expose Prefect's standard ports (if running agents/workers in the container)
EXPOSE 4200
EXPOSE 8080

# Set up default entrypoint (this can be overridden for specific workflows)
ENTRYPOINT ["prefect"]

# Optionally specify a default CMD to launch a Prefect worker (replace as needed)
#CMD ["worker", "start", "--pool", "kubernetes"]
