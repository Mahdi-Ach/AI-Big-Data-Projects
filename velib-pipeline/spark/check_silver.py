from pyspark.sql import SparkSession
from pyspark.sql.functions import col

S3 = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "minioadmin",
    "spark.hadoop.fs.s3a.secret.key": "minioadmin123",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.aws.credentials.provider":
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
}

builder = (
    SparkSession.builder
    .appName("check-silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
for k, v in S3.items():
    builder = builder.config(k, v)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

df = spark.read.format("delta").load("s3a://silver/station_status_enriched")

print("\n===== SILVER station_status_enriched =====")
print(f"Lignes    : {df.count()}")
print(f"Colonnes  : {df.columns}")
print("\nSchéma :")
df.printSchema()

print("\nStatistiques qualité :")
total = df.count()
nulls_name = df.filter(col("name").isNull()).count()
nulls_capacity = df.filter(col("capacity").isNull()).count()
neg_bikes = df.filter(col("num_bikes_available") < 0).count()
neg_docks = df.filter(col("num_docks_available") < 0).count()
duplicates = total - df.dropDuplicates(["station_id", "ingested_at"]).count()

print(f"  Lignes totales                : {total}")
print(f"  name NULL (jointure ratée)    : {nulls_name}")
print(f"  capacity NULL (jointure ratée): {nulls_capacity}")
print(f"  num_bikes_available < 0       : {neg_bikes}")
print(f"  num_docks_available < 0       : {neg_docks}")
print(f"  Doublons (station_id+ingested): {duplicates}")

print("\nRépartition par date :")
df.groupBy("date").count().orderBy("date").show()

print("\nTop 5 stations avec le plus de vélos :")
df.orderBy(col("num_bikes_available").desc()).select(
    "station_id", "name", "num_bikes_available", "capacity", "taux_remplissage", "date"
).show(5, truncate=False)

spark.stop()
