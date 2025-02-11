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

# Set npm global install directory to avoid permission issues
RUN mkdir -p /usr/local/npm-global && \
    npm config set prefix '/usr/local/npm-global' && \
    export PATH="/usr/local/npm-global/bin:$PATH"

# Install mapshaper-xl
RUN npm install -g mapshaper && \
    ln -s /usr/local/npm-global/bin/mapshaper /usr/local/bin/mapshaper-xl

# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

# Upgrade pip and install Python dependencies
RUN pip install --upgrade pip && \
    pip install mlflow==2.18.0

# Install geopandas and related dependencies
RUN pip install geopandas==0.13.2 kubernetes==31.0.0

# Install additional Python packages
RUN pip install jsonschema==4.19.0 zipfile36==0.1.3 psycopg2==2.9.10

# Set up git-lfs and SSH
RUN git lfs install --system && \
    ssh-keygen -A  # Ensures SSH host keys are generated if needed
