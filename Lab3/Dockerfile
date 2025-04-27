FROM bitnami/spark:3.5.0

USER root

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    wget \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir \
    pyspark==3.5.0 \
    delta-spark==3.0.0 \
    mlflow==2.8.0 \
    pandas \
    scikit-learn \
    matplotlib

RUN mkdir -p /app/src /app/data /app/notebooks /app/logs
WORKDIR /app
