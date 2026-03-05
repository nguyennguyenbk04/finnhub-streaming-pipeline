FROM apache/spark:3.5.8-python3

USER root
WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# spark-sql-kafka  – Kafka source/sink for Structured Streaming
# spark-cassandra-connector – foreachBatch write to Cassandra
ENTRYPOINT ["/opt/spark/bin/spark-submit", \
  "--master", "local[*]", \
  "--packages", \
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.5.0"]

CMD ["src/processor.py"]

