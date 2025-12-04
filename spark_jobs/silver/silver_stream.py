from pyspark.sql import SparkSession
from pyspark.sql.functions import col, unix_timestamp, when

def create_spark_session():
    packages = ",".join([
        "io.delta:delta-spark_2.13:4.0.0"
    ])

    spark = (
        SparkSession.builder.appName("SilverTransform")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )
    return spark


def main():
    spark = create_spark_session()

    bronze_path = "data/bronze/rides"
    silver_path = "data/silver/rides_clean"

    print("📌 Reading Bronze table...")
    df = spark.read.format("delta").load(bronze_path)

    print("📌 Cleaning & enriching data...")

    # ---- Fix: correct duration calculation ----
    df = df.withColumn(
        "ride_duration_minutes",
        (unix_timestamp(col("end_time_ts")) - unix_timestamp(col("start_time_ts"))) / 60
    )

    # Filter invalid durations
    df = df.filter(col("ride_duration_minutes") > 0)

    # Price category bucket
    df = df.withColumn(
        "price_bucket",
        when(col("price") > 30, "high").otherwise("normal")
    )

    print("📌 Writing Silver Delta table...")
    df.write.format("delta").mode("overwrite").save(silver_path)

    print(f"✨ Silver table written to: {silver_path}")


if __name__ == "__main__":
    main()
