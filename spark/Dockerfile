FROM apache/spark:3.5.6-java17-python3

USER root

# Args for user mapping
ARG UID=1000
ARG GID=1000

# Args for versions
ARG SPARK_VERSION=3.5.6
ARG ICEBERG_VERSION=1.5.2
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262

# System dependencies (minimal)
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        software-properties-common \
        curl \
        procps \
        openjdk-17-jdk && \
    add-apt-repository ppa:deadsnakes/ppa -y && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
        python3.11 \
        python3.11-venv && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Download Jars
RUN mkdir -p /opt/spark/jars && \
    curl -L -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar && \
    curl -L -o /opt/spark/jars/iceberg-aws-bundle-${ICEBERG_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    curl -L -o /opt/spark/jars/hadoop-aws-${HADOOP_AWS_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar && \
    curl -L -o /opt/spark/jars/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar && \
    curl -L -o /opt/spark/jars/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar \
        https://repo1.maven.org/maven2/org/apache/spark/spark-hadoop-cloud_2.12/${SPARK_VERSION}/spark-hadoop-cloud_2.12-${SPARK_VERSION}.jar


# Copy config
COPY ./conf/ /opt/spark/conf/

# Copy requirements
COPY requirements.txt /tmp/requirements.txt

# Install Python + Jupyter
RUN pip install --no-cache-dir \
        "notebook<7.0" \
        "jupyterlab" \
        ipykernel \
    && pip install --no-cache-dir -r /tmp/requirements.txt

# Add Jupyter kernel globally (NOT --user!)
RUN python3 -m ipykernel install --name spark-python --display-name "Spark Py3"

# Environment variables
ENV SPARK_HOME=/opt/spark \
    PYSPARK_PYTHON=python3 \
    PYSPARK_DRIVER_PYTHON=python3


# Create workspace & set permissions
RUN mkdir -p /home/spark/work && \
    chown -R spark:spark /home/spark && \
    chown -R spark:spark /opt/spark

USER spark
WORKDIR /home/spark/work

EXPOSE 4040 8080 7077 8888

CMD ["bash", "-c", "jupyter lab --ip=0.0.0.0 --port=8888 --no-browser --NotebookApp.token=\"$JUPYTER_TOKEN\" --NotebookApp.password=\"$JUPYTER_PASSWORD\""]