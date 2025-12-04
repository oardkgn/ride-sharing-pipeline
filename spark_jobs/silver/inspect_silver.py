from pyspark.sql import SparkSession

packages = "io.delta:delta-spark_2.13:4.0.0"

spark = (
    SparkSession.builder.appName("CheckSilver")
    .config("spark.jars.packages", packages)
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

df = spark.read.format("delta").load("data/silver/rides_clean")
df.show(3, truncate=False)
print("Row count:", df.count())
df.printSchema()
