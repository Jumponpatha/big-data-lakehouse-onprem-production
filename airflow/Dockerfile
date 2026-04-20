ARG PY_SPARK_VERSION=latest
FROM spark-prod:${PY_SPARK_VERSION} AS build

FROM apache/airflow:3.1.6-python3.11

USER root

# Install Java 11 (match Spark image)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        openjdk-17-jdk \
        curl \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy ONLY spark runtime
COPY --from=build /opt/spark /opt/spark

# SSH for remote cluster (if needed)
# RUN mkdir -p /home/airflow/.ssh && \
#     chown airflow:root /home/airflow/.ssh && \
#     chmod 700 /home/airflow/.ssh

RUN mkdir -p /opt/spark/logs && \
    chmod -R 777 /opt/spark/logs

# COPY --chown=airflow:root dockerkey /home/airflow/.ssh/id_rsa
# RUN chmod 600 /home/airflow/.ssh/id_rsa

USER airflow

# Python dependencies
COPY requirements.txt /tmp/requirements.txt

# Install Airflow providers and Spark dependencies
RUN pip install --no-cache-dir --upgrade pip setuptools wheel && \
    pip install --no-cache-dir pyspark==3.5.6 && \
    pip install --no-cache-dir apache-airflow-providers-apache-spark==4.1.0 && \
    pip install --no-cache-dir psycopg2-binary && \
    PIP_PREFER_BINARY=1 pip install --no-cache-dir -r /tmp/requirements.txt
COPY dags/ /opt/airflow/dags

WORKDIR /opt/airflow