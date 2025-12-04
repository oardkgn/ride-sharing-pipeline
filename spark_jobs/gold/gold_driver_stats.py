from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
    when,
)


def create_spark_session():
    """
    Spark session with Delta support for batch processing.
    """
    packages = "io.delta:delta-spark_2.13:4.0.0"

    spark = (
        SparkSession.builder.appName("GoldDriverStats")
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

    silver_path = "../data/silver/rides_clean"
    gold_path = "../data/gold/driver_stats"

    print("📌 Reading Silver table...")
    df = spark.read.format("delta").load(silver_path)

    print("📌 Aggregating per driver...")
    agg = (
        df.groupBy("driver_id")
        .agg(
            spark_count("*").alias("total_rides"),
            spark_sum("price").alias("total_revenue"),
            spark_avg("price").alias("avg_price"),
            spark_avg("distance_km").alias("avg_distance_km"),
            spark_avg("ride_duration_minutes").alias(
                "avg_ride_duration_minutes"
            ),
            spark_sum(
                when(col("status") == "completed", 1).otherwise(0)
            ).alias("completed_rides"),
        )
    )

    # Add completion rate as a derived metric
    agg = agg.withColumn(
        "completion_rate", col("completed_rides") / col("total_rides")
    )

    print("📊 Sample driver stats:")
    agg.show(20, truncate=False)

    print("📌 Writing Gold Delta table (driver_stats)...")
    (
        agg.write.format("delta")
        .mode("overwrite")  # snapshot based on current Silver
        .save(gold_path)
    )

    print(f"✨ Gold table written to: {gold_path}")


if __name__ == "__main__":
    main()
