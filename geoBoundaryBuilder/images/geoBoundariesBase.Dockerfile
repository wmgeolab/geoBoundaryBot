FROM python:3.11-slim

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

# Install system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    musl-dev \
    gcc \
    g++ \
    libffi-dev \
    libssl-dev \
    make \
    git \
    git-lfs \
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
    lsb-release && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install prefect==3.1.5 kubernetes==25.3.0

# Install geopandas and related dependencies
RUN pip install geopandas==0.13.2

# Install additional Python packages
RUN pip install jsonschema==4.19.0 zipfile36==0.1.3

# Install Prefect Kubernetes components
RUN pip install prefect-kubernetes==0.5.3

# Set up git-lfs
RUN git lfs install