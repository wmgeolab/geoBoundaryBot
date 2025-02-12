FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    GDAL_VERSION=3.6.2 \
    GDAL_DATA=/usr/share/gdal \
    PROJ_LIB=/usr/share/proj

# Install system dependencies in a single layer
RUN apt-get update && apt-get install -y --no-install-recommends \
    musl-dev \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    make \
    git \
    git-lfs \
    openssh-client \
    gdal-bin \
    libgdal-dev \
    libgeos-dev \
    libproj-dev \
    libjpeg-dev \
    zlib1g-dev \
    libcairo2-dev \
    libpq-dev \
    curl \
    apt-transport-https \
    ca-certificates \
    gnupg \
    lsb-release \
    nodejs \
    npm && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Mapshaper-xl
RUN npm install -g mapshaper && \
    [ ! -f /usr/local/bin/mapshaper-xl ] && ln -s /usr/local/bin/mapshaper /usr/local/bin/mapshaper-xl || true


# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies in a single layer
RUN pip install --upgrade pip && \
    pip install \
    geopandas==0.13.2 \
    fiona==1.9.5 \
    shapely \
    rasterio \
    pyproj \
    mlflow==2.18.0 \
    kubernetes==31.0.0 \
    jsonschema==4.19.0 \
    zipfile36==0.1.3 \
    psycopg2==2.9.10

# Set up git-lfs and SSH
RUN git lfs install --system && \
    ssh-keygen -A  # Ensures SSH host keys are generated if needed
