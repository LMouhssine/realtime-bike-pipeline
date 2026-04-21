from __future__ import annotations

import logging
import sys
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.append(str(PROJECT_ROOT))

from config.settings import Settings, load_settings
LOGGER = logging.getLogger("citybike.spark")


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )


def build_station_schema() -> StructType:
    return StructType(
        [
            StructField("station_id", StringType(), True),
            StructField("station_name", StringType(), True),
            StructField("latitude", DoubleType(), True),
            StructField("longitude", DoubleType(), True),
            StructField("bikes_available", IntegerType(), True),
            StructField("free_slots", IntegerType(), True),
            StructField("timestamp", StringType(), True),
            StructField("network_id", StringType(), True),
            StructField("city", StringType(), True),
            StructField("station_key", StringType(), True),
            StructField("ingested_at", StringType(), True),
        ]
    )


def parse_timestamp(column_name: str) -> F.Column:
    sanitized = F.regexp_replace(F.trim(F.col(column_name)), r"([+-]\d{2}:?\d{2})Z$", r"\1")
    return F.coalesce(
        F.to_timestamp(sanitized),
        F.to_timestamp(sanitized, "yyyy-MM-dd'T'HH:mm:ssXXX"),
        F.to_timestamp(sanitized, "yyyy-MM-dd'T'HH:mm:ss.SSSXXX"),
        F.to_timestamp(sanitized, "yyyy-MM-dd HH:mm:ss"),
    )


def clean_station_frame(df: DataFrame) -> DataFrame:
    transformed = (
        df.withColumn("latitude", F.col("latitude").cast(DoubleType()))
        .withColumn("longitude", F.col("longitude").cast(DoubleType()))
        .withColumn("bikes_available", F.col("bikes_available").cast(IntegerType()))
        .withColumn("free_slots", F.col("free_slots").cast(IntegerType()))
        .withColumn("snapshot_timestamp", parse_timestamp("timestamp"))
        .withColumn("ingested_at", parse_timestamp("ingested_at"))
        .drop("timestamp")
    )

    cleaned = (
        transformed.filter(F.col("station_key").isNotNull())
        .filter(F.col("station_id").isNotNull())
        .filter(F.col("station_name").isNotNull())
        .filter(F.col("city").isNotNull())
        .filter(F.col("network_id").isNotNull())
        .filter(F.col("snapshot_timestamp").isNotNull())
        .filter(F.col("ingested_at").isNotNull())
        .filter(F.col("latitude").isNotNull() & F.col("longitude").isNotNull())
        .filter(F.col("bikes_available").isNotNull() & F.col("free_slots").isNotNull())
        .filter((F.col("bikes_available") >= 0) & (F.col("free_slots") >= 0))
        .withColumn("capacity", F.col("bikes_available") + F.col("free_slots"))
        .withColumn(
            "utilization_rate",
            F.when(
                F.col("capacity") > 0,
                F.col("bikes_available") / F.col("capacity"),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "zone_id",
            F.concat(
                F.format_string("%.2f", F.round(F.col("latitude"), 2)),
                F.lit("_"),
                F.format_string("%.2f", F.round(F.col("longitude"), 2)),
            ),
        )
        .withColumn("event_date", F.to_date("snapshot_timestamp"))
        .withColumn("event_hour", F.hour("snapshot_timestamp"))
    )

    return cleaned.select(
        "station_key",
        "network_id",
        "city",
        "station_id",
        "station_name",
        "latitude",
        "longitude",
        "bikes_available",
        "free_slots",
        "capacity",
        "utilization_rate",
        "zone_id",
        "snapshot_timestamp",
        "ingested_at",
        "event_date",
        "event_hour",
    )


def transform_station_frame(df: DataFrame) -> DataFrame:
    cleaned = clean_station_frame(df)
    return (
        cleaned.withWatermark("snapshot_timestamp", "10 minutes")
        .dropDuplicates(["station_key", "snapshot_timestamp"])
    )


def create_spark_session(settings: Settings) -> SparkSession:
    spark = (
        SparkSession.builder.appName(settings.spark_app_name)
        .master(settings.spark_master_url)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def run_streaming_job() -> None:
    from storage.db_writer import DbWriter

    configure_logging()
    settings = load_settings()
    writer = DbWriter(settings)
    writer.bootstrap_schema()

    spark = create_spark_session(settings)
    schema = build_station_schema()

    LOGGER.info(
        "Starting Spark streaming job for topic=%s, checkpoint=%s",
        settings.kafka_topic,
        settings.spark_checkpoint_dir,
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", settings.kafka_starting_offsets)
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed_stream = (
        raw_stream.select(
            F.from_json(F.col("value").cast("string"), schema).alias("record"),
        )
        .select("record.*")
    )

    transformed_stream = transform_station_frame(parsed_stream)

    query = (
        transformed_stream.writeStream.outputMode("append")
        .queryName("citybike_station_processing")
        .option("checkpointLocation", settings.spark_checkpoint_dir)
        .trigger(processingTime=f"{settings.poll_interval_seconds} seconds")
        .foreachBatch(writer.write_micro_batch)
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run_streaming_job()
