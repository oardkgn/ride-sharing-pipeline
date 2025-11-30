from pyspark.sql import SparkSession

def create_spark():
    packages = ",".join([
        "io.delta:delta-spark_2.13:4.0.0",
        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0",
        "org.apache.kafka:kafka-clients:3.5.1",
    ])
    return (
        SparkSession.builder.appName("InspectBronze")
        .config("spark.jars.packages", packages)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .getOrCreate()
    )

def main():
    spark = create_spark()
    df = spark.read.format("delta").load("data/bronze/rides")
    df.show(10, truncate=False)
    df.printSchema()

if __name__ == "__main__":
    main()
