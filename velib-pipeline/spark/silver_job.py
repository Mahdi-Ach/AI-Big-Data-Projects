# spark/silver_job.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    current_timestamp,
    explode,
    from_json,
    lit,
    row_number,
    to_date,
    when,
)
from pyspark.sql.types import (
    ArrayType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
)
from pyspark.sql.window import Window

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
    .appName("velib-silver")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
for k, v in S3.items():
    builder = builder.config(k, v)
spark = builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# ----- Schémas GBFS -----
info_schema = StructType([
    StructField("data", StructType([
        StructField("stations", ArrayType(StructType([
            StructField("station_id", StringType()),
            StructField("name", StringType()),
            StructField("lat", DoubleType()),
            StructField("lon", DoubleType()),
            StructField("capacity", LongType()),
            StructField("stationCode", StringType()),
        ])))
    ]))
])

status_schema = StructType([
    StructField("data", StructType([
        StructField("stations", ArrayType(StructType([
            StructField("station_id", StringType()),
            StructField("num_bikes_available", LongType()),
            StructField("num_docks_available", LongType()),
            StructField("is_installed", LongType()),
            StructField("is_renting", LongType()),
            StructField("is_returning", LongType()),
            StructField("last_reported", LongType()),
        ])))
    ]))
])

# ----- Lecture Bronze -----
info_bronze = spark.read.format("delta").load("s3a://bronze/station_information")
status_bronze = spark.read.format("delta").load("s3a://bronze/station_status")

info = (
    info_bronze
    .withColumn("json", from_json(col("payload_raw"), info_schema))
    .select(explode(col("json.data.stations")).alias("s"), col("ingested_at"))
    .select(
        col("s.station_id").alias("station_id"),
        col("s.name").alias("name"),
        col("s.lat").alias("lat"),
        col("s.lon").alias("lon"),
        col("s.capacity").alias("capacity"),
        col("s.stationCode").alias("station_code"),
        col("ingested_at"),
    )
    .filter(col("station_id").isNotNull())
    .filter((col("lat").between(48.7, 49.0)) & (col("lon").between(2.2, 2.6)))
    .dropDuplicates(["station_id"])
)

status = (
    status_bronze
    .withColumn("json", from_json(col("payload_raw"), status_schema))
    .select(explode(col("json.data.stations")).alias("s"), col("ingested_at"))
    .select(
        col("s.station_id").alias("station_id"),
        col("s.num_bikes_available").alias("num_bikes_available"),
        col("s.num_docks_available").alias("num_docks_available"),
        col("s.is_installed").alias("is_installed"),
        col("s.is_renting").alias("is_renting"),
        col("s.is_returning").alias("is_returning"),
        col("s.last_reported").alias("last_reported"),
        col("ingested_at"),
    )
    .filter(col("station_id").isNotNull())
    .filter(col("num_bikes_available") >= 0)
    .filter(col("num_docks_available") >= 0)
)

# Dédoublonnage : garder la ligne la plus récente par station_id + ingested_at
w = Window.partitionBy("station_id", "ingested_at").orderBy(col("last_reported").desc())
status_dedup = (
    status.withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

silver = (
    status_dedup.alias("st")
    .join(info.alias("inf"), "station_id", "left")
    .select(
        col("st.station_id"),
        col("inf.name"),
        col("inf.lat"),
        col("inf.lon"),
        col("inf.capacity"),
        col("st.num_bikes_available"),
        col("st.num_docks_available"),
        col("st.is_installed"),
        col("st.is_renting"),
        col("st.is_returning"),
        col("st.last_reported"),
        col("st.ingested_at"),
        to_date(col("st.ingested_at")).alias("date"),
    )
    .withColumn("taux_remplissage",
                when(col("capacity") > 0,
                     col("num_bikes_available") / col("capacity"))
                .otherwise(lit(None)))
    .withColumn("silver_ts", current_timestamp())
)

# ----- Écriture partitionnée par date -----
(
    silver.write
    .format("delta")
    .mode("append")
    .partitionBy("date")
    .save("s3a://silver/station_status_enriched")
)

print(f"Silver écrit : {silver.count()} lignes")
spark.stop()
