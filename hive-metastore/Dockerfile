FROM apache/hive:4.0.0
USER root

RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

# hadoop-aws + AWS SDK bundle for S3A / MinIO access
# Versions must match Hadoop 3.3.6 shipped in apache/hive:4.0.0
RUN curl -fsSL \
    "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
    -o /opt/hive/lib/hadoop-aws-3.3.6.jar && \
    curl -fsSL \
    "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar" \
    -o /opt/hive/lib/aws-java-sdk-bundle-1.12.367.jar

COPY conf/core-site.xml /opt/hive/conf/core-site.xml

USER hive
