FROM apache/hive:4.0.0

USER root
# Install curl
RUN apt-get update \
    && apt-get install -y netcat curl \
    && apt-get install -y postgresql-client curl \
    && rm -rf /var/lib/apt/lists/*

# Download PostgreSQL JDBC driver
RUN curl -L -o /opt/hive/lib/postgresql.jar \
        https://jdbc.postgresql.org/download/postgresql-42.7.3.jar && \
    curl -L -o /opt/hive/lib/hadoop-aws-3.3.4.jar \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar && \
    curl -L -o /opt/hive/lib/iceberg-hive-runtime-1.7.0.jar \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-hive-runtime/1.7.0/iceberg-hive-runtime-1.7.0.jar

RUN chmod +x /entrypoint.sh

COPY entrypoint.sh /entrypoint.sh

USER hive
ENTRYPOINT ["/entrypoint.sh"]
