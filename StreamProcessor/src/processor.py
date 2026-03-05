import os
import logging
import pyspark.sql.functions as F

from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, LongType, ArrayType,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

# --------------- Config (injected by docker-compose) ---------------
KAFKA_BROKER   = os.getenv("KAFKA_BROKER",    "kafka:9092")
KAFKA_TOPIC    = os.getenv("KAFKA_TOPIC",     "finnhub-trades")
CASSANDRA_HOST = os.getenv("CASSANDRA_HOST",  "cassandra")
CASSANDRA_USER = os.getenv("CASSANDRA_USER",  "cassandra")
CASSANDRA_PASS = os.getenv("CASSANDRA_PASS",  "cassandra")
CASSANDRA_KS   = os.getenv("CASSANDRA_KEYSPACE", "trading")

# --------------- Spark Session ---------------
spark = (
    SparkSession.builder
    .appName("FinnhubStreamProcessor")
    .config("spark.cassandra.connection.host", CASSANDRA_HOST)
    .config("spark.cassandra.auth.username",   CASSANDRA_USER)
    .config("spark.cassandra.auth.password",   CASSANDRA_PASS)
    .config("spark.sql.shuffle.partitions",    "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

# --------------- Incoming JSON schema (matches producer.py) ---------------
trade_schema = StructType([
    StructField("symbol",     StringType(),            True),
    StructField("price",      DoubleType(),            True),
    StructField("volume",     DoubleType(),            True),
    StructField("timestamp",  LongType(),              True),   # Unix ms
    StructField("conditions", ArrayType(StringType()), True),
])

# --------------- Read from Kafka ---------------
raw_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BROKER)
    .option("subscribe",               KAFKA_TOPIC)
    .option("startingOffsets",         "latest")
    .load()
)

# Parse JSON value and derive trade_time / trade_date columns
parsed_df = (
    raw_df
    .select(F.from_json(F.col("value").cast("string"), trade_schema).alias("d"))
    .select("d.*")
    .withColumn("trade_time", (F.col("timestamp") / 1000).cast("timestamp"))
    .withColumn("trade_date", F.col("trade_time").cast("date"))
    .withColumn("conditions", F.coalesce(F.col("conditions"), F.array()))
    .drop("timestamp")
)

# ── Query 1: raw trades → trading.trades ──────────────────────────────────
# Mirrors example project's query1: continuous foreachBatch into Cassandra.
def write_trades(batch_df, batch_id):
    logger.info("[Trades] Writing batch %s to Cassandra", batch_id)
    (
        batch_df
        .select("symbol", "trade_date", "trade_time", "price", "volume", "conditions")
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(table="trades", keyspace=CASSANDRA_KS)
        .mode("append")
        .save()
    )

query1 = (
    parsed_df.writeStream
    .foreachBatch(write_trades)
    .outputMode("update")
    .start()
)

# ── Query 2: 1-minute OHLCV aggregations → trading.trades_agg_1m ──────────
# Mirrors example project's query2: 5-second trigger with watermark.
# open = first trade price in window, close = last trade price.
agg_df = (
    parsed_df
    .withWatermark("trade_time", "1 minute")
    .groupBy(
        F.col("symbol"),
        F.col("trade_date"),
        F.window(F.col("trade_time"), "1 minute").alias("w"),
    )
    .agg(
        F.min_by("price", "trade_time").alias("open_price"),
        F.max_by("price", "trade_time").alias("close_price"),
        F.max("price").alias("high_price"),
        F.min("price").alias("low_price"),
        F.sum("volume").alias("total_volume"),
        F.count("*").cast("int").alias("trade_count"),
        F.avg("price").alias("avg_price"),
    )
    .withColumn("window_start", F.col("w.start"))
    .drop("w")
)

def write_agg(batch_df, batch_id):
    logger.info("[Agg 1m] Writing batch %s to Cassandra", batch_id)
    (
        batch_df
        .select(
            "symbol", "trade_date", "window_start",
            "open_price", "close_price", "high_price",
            "low_price", "total_volume", "trade_count", "avg_price",
        )
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(table="trades_agg_1m", keyspace=CASSANDRA_KS)
        .mode("append")
        .save()
    )

query2 = (
    agg_df.writeStream
    .trigger(processingTime="5 seconds")
    .foreachBatch(write_agg)
    .outputMode("update")
    .start()
)

# ── Query 3: latest price per symbol → trading.latest_price ───────────────
# Upserts the most recent trade for each symbol (PRIMARY KEY is symbol).
def write_latest(batch_df, batch_id):
    if batch_df.isEmpty():
        return
    logger.info("[Latest] Writing batch %s to Cassandra", batch_id)
    latest_df = (
        batch_df
        .groupBy("symbol")
        .agg(
            F.max("trade_time").alias("trade_time"),
            F.max_by("price",  "trade_time").alias("price"),
            F.max_by("volume", "trade_time").alias("volume"),
        )
    )
    (
        latest_df
        .select("symbol", "trade_time", "price", "volume")
        .write
        .format("org.apache.spark.sql.cassandra")
        .options(table="latest_price", keyspace=CASSANDRA_KS)
        .mode("append")
        .save()
    )

query3 = (
    parsed_df.writeStream
    .foreachBatch(write_latest)
    .outputMode("update")
    .start()
)

# --------------- Await termination ---------------
spark.streams.awaitAnyTermination()
