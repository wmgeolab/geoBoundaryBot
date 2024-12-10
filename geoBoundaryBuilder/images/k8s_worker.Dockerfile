# Start from the Prefect image
FROM prefecthq/prefect:3.1.6.dev6-python3.12-kubernetes

# Install Docker
USER root

# Install system dependencies and Docker
RUN apt-get update && \
    apt-get install -y \
    apt-transport-https \
    ca-certificates \
    curl \
    gnupg \
    lsb-release \
    sudo \
    && curl -fsSL https://download.docker.com/linux/debian/gpg | tee /etc/apt/trusted.gpg.d/docker.asc \
    && echo "deb [arch=amd64] https://download.docker.com/linux/debian $(lsb_release -cs) stable" | tee /etc/apt/sources.list.d/docker.list \
    && apt-get update && apt-get install -y \
    docker-ce-cli \
    docker-ce \
    docker-compose \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Set up Docker group
RUN groupadd docker && usermod -aG docker $USER

# Configure the user to have permission to run Docker commands
RUN mkdir /etc/sudoers.d && \
    echo "$USER ALL=(ALL) NOPASSWD: /usr/bin/docker" > /etc/sudoers.d/docker

# Set the working directory and set the default command
WORKDIR /workspace

# Revert back to a non-root user (assuming $USER is a non-root user in the Prefect image)
USER $USER

# Optionally, expose Docker socket if needed (to share Docker daemon)
VOLUME ["/var/run/docker.sock:/var/run/docker.sock"]

