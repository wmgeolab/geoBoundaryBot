# Start from the Prefect image
FROM prefecthq/prefect:3.1.6.dev6-python3.12-kubernetes

RUN apt-get update && apt-get install -y --no-install-recommends \
                gnupg \
                lsb-release && \
                apt-get clean && rm -rf /var/lib/apt/lists/*


# Install Docker CLI
RUN curl -fsSL https://download.docker.com/linux/debian/gpg | gpg --dearmor -o /usr/share/keyrings/docker-archive-keyring.gpg && \
    echo "deb [arch=$(dpkg --print-architecture) signed-by=/usr/share/keyrings/docker-archive-keyring.gpg] https://download.docker.com/linux/debian $(lsb_release -cs) stable" > /etc/apt/sources.list.d/docker.list && \
    apt-get update && apt-get install -y --no-install-recommends \
    docker-ce-cli && \
    apt-get clean && rm -rf /var/lib/apt/lists/*