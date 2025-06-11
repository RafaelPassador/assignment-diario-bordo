# Dockerfile
FROM bitnami/spark:3.5.1

WORKDIR /app

COPY . .

USER root

# Instala Python 3 + pip
RUN apt-get update && \
    apt-get install -y python3 python3-pip && \
    rm -rf /var/lib/apt/lists/*

# Instala PySpark (já incluso no Spark, mas para garantir compatibilidade do Python)
RUN pip3 install --upgrade pip && \
    pip3 install pyspark

# Instala Delta Lake compatível com Spark 3.5.1
RUN pip3 install delta-spark==3.2.0

RUN pip install --no-cache-dir -r requirements.txt

# Configura variáveis de ambiente
ENV JAVA_HOME=/opt/bitnami/java
ENV PYSPARK_PYTHON=python3
ENV SPARK_VERSION=3.5.1
ENV PATH=$PATH:/home/spark/.local/bin

USER 1001 
