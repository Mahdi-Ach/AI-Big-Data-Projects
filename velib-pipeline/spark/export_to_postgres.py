# spark/export_to_postgres.py
from pyspark.sql import SparkSession

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
    .appName("export-silver-to-pg")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
for k, v in S3.items():
    builder = builder.config(k, v)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Ajouter le driver JDBC Postgres au classpath
# On suppose que postgresql-42.x.jar est dans /opt/spark/jars-extra
# (on le téléchargera juste après)
silver = spark.read.format("delta").load("s3a://silver/station_status_enriched")

(
    silver.write
    .format("jdbc")
    .option("url", "jdbc:postgresql://postgres:5432/velib")
    .option("dbtable", "silver_station_status_enriched")
    .option("user", "velib")
    .option("password", "velib123")
    .option("driver", "org.postgresql.Driver")
    .mode("overwrite")
    .save()
)

print(f"Exporté {silver.count()} lignes vers Postgres.silver_station_status_enriched")
spark.stop()
