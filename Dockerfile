# Dockerfile
FROM deltaio/delta-docker:0.8.1_2.3.0

WORKDIR /app

COPY . .

USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir -r requirements.txt