from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    to_date,
    sum as spark_sum,
    avg as spark_avg,
    count as spark_count,
)


def create_spark_session():
    """
    Spark session with Delta support for batch processing.
    """
    packages = "io.delta:delta-spark_2.13:4.0.0"

    spark = (
        SparkSession.builder.appName("GoldRideStatusDaily")
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
    gold_path = "data/gold/ride_status_daily"

    print("📌 Reading Silver table...")
    df = spark.read.format("delta").load(silver_path)

    # 1️⃣ Extract date from timestamp
    df_with_date = df.withColumn("ride_date", to_date(col("created_at_ts")))

    # 2️⃣ Aggregate by date + status
    agg = (
        df_with_date.groupBy("ride_date", "status")
        .agg(
            spark_count("*").alias("total_rides"),
            spark_avg("price").alias("avg_price"),
            spark_avg("distance_km").alias("avg_distance_km"),
            spark_avg("ride_duration_minutes").alias("avg_ride_duration_minutes"),
        )
        .orderBy("ride_date", "status")
    )

    print("📊 Sample daily status summary:")
    agg.show(20, truncate=False)

    print("📌 Writing Gold Delta table (ride_status_daily)...")
    (
        agg.write.format("delta")
        .mode("overwrite")  # snapshot for current Silver
        .save(gold_path)
    )

    print(f"✨ Gold table written to: {gold_path}")


if __name__ == "__main__":
    main()
