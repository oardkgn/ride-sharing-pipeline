from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    date_trunc,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
)

def create_spark_session():
    """
    Simple Spark session with Delta support for batch processing.
    No Kafka needed here – we're just reading Silver from disk.
    """
    packages = "io.delta:delta-spark_2.13:4.0.0"

    spark = (
        SparkSession.builder.appName("GoldHourlyRevenue")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")
    return spark


def main():
    spark = create_spark_session()

    silver_path = "data/silver/rides_clean"
    gold_path = "data/gold/hourly_revenue"

    print("📌 Reading Silver table...")
    df = spark.read.format("delta").load(silver_path)

    # 1️⃣ Create an 'hour' column by truncating to hour
    df_hourly = df.withColumn(
        "event_hour", date_trunc("hour", col("created_at_ts"))
    )

    # 2️⃣ Aggregate metrics per hour
    agg = (
        df_hourly.groupBy("event_hour")
        .agg(
            spark_count("*").alias("total_rides"),
            spark_sum("price").alias("total_revenue"),
            spark_avg("price").alias("avg_price"),
            spark_sum(
                (col("status") == "completed").cast("int")
            ).alias("completed_rides"),
            spark_sum(
                (col("status") == "requested").cast("int")
            ).alias("requested_rides"),
        )
        .orderBy("event_hour")
    )

    print("📊 Sample of hourly metrics:")
    agg.show(20, truncate=False)

    print("📌 Writing Gold Delta table (hourly_revenue)...")
    (
        agg.write.format("delta")
        .mode("overwrite")   # snapshot of current Silver
        .save(gold_path)
    )

    print(f"✨ Gold table written to: {gold_path}")


if __name__ == "__main__":
    main()
