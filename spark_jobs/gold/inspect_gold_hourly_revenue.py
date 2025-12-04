from pyspark.sql import SparkSession

packages = "io.delta:delta-spark_2.13:4.0.0"

spark = (
    SparkSession.builder.appName("InspectGold")
    .config("spark.jars.packages", packages)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .getOrCreate()
)

gold = spark.read.format("delta").load("data/gold/hourly_revenue")
gold.show(40, truncate=False)
gold.printSchema()
