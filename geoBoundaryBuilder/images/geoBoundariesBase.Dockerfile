FROM alpine:3.18

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Update the package index and install dependencies
RUN apk add --no-cache \
    python3 \
    py3-pip \
    python3-dev \
    musl-dev \
    gcc \
    g++ \
    libffi-dev \
    openssl-dev \
    make \
    git \
    git-lfs \
    gdal \
    gdal-dev \
    geos-dev \
    proj-dev \
    jpeg-dev \
    zlib-dev \
    cairo-dev \
    py3-cffi \
    py3-psycopg2

# Install Python packages in steps for debugging
RUN pip install --upgrade pip && \
    pip install prefect==2.2.0 kubernetes==25.3.0

RUN pip install geopandas==0.13.2 shapely==2.0.1 matplotlib==3.7.2 pandas==2.1.1

RUN pip install jsonschema==4.19.0 zipfile36==0.1.3

# Install Prefect Kubernetes components
RUN pip install prefect-kubernetes

# Set up git-lfs
RUN git lfs install