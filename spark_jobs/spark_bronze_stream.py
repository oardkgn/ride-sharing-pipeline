import json
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, TimestampType
)

def create_spark_session(app_name: str = "RideSharingBronzeIngest"):
    """
    Create a SparkSession configured for:
    - Delta Lake
    - Kafka

    We explicitly tell Spark which JAR packages to download:
    - io.delta:delta-spark_2.13:4.0.0   (Delta Lake for Spark 3.5 / Scala 2.13)
    - org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0   (Kafka connector)
    - org.apache.kafka:kafka-clients:3.5.1               (Kafka client lib)
    """
    packages = ",".join([
        "io.delta:delta-spark_2.13:4.0.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
        "org.apache.kafka:kafka-clients:3.5.1",
    ])

    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def get_event_schema():
    """
    Schema must match what we send from ride_event_generator.py.
    """
    return StructType(
        [
            StructField("ride_id", StringType(), True),
            StructField("passenger_id", StringType(), True),
            StructField("driver_id", StringType(), True),
            StructField("status", StringType(), True),
            StructField("distance_km", DoubleType(), True),
            StructField("price", DoubleType(), True),
            StructField("start_time", StringType(), True),
            StructField("end_time", StringType(), True),
            StructField("created_at", StringType(), True),
        ]
    )


def main():
    spark = create_spark_session()

    kafka_bootstrap = "localhost:9092"
    topic = "ride_events"

    # 1️⃣ Read stream from Kafka
    kafka_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")  # read from beginning
        .load()
    )

    # Kafka value is bytes → cast to string
    raw_df = kafka_df.selectExpr("CAST(value AS STRING) as json_str")

    # 2️⃣ Parse JSON into columns using schema
    schema = get_event_schema()
    parsed_df = raw_df.select(
        from_json(col("json_str"), schema).alias("data")
    ).select("data.*")

    # Optional: cast time columns to TimestampType
    parsed_df = (
        parsed_df
        .withColumn("start_time_ts", col("start_time").cast(TimestampType()))
        .withColumn("end_time_ts", col("end_time").cast(TimestampType()))
        .withColumn("created_at_ts", col("created_at").cast(TimestampType()))
    )

    # 3️⃣ Write to Delta (Bronze)
    bronze_path = "data/bronze/rides"
    checkpoint_path = "checkpoints/bronze_rides"

    query = (
        parsed_df.writeStream.format("delta")
        .option("checkpointLocation", checkpoint_path)
        .outputMode("append")
        .start(bronze_path)
    )

    print("🔥 Spark Bronze stream started. Writing to:", bronze_path)
    print("   Press Ctrl+C to stop.")

    query.awaitTermination()


if __name__ == "__main__":
    main()
