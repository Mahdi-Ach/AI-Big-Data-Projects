# spark/bronze_streaming.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, from_json
from pyspark.sql.types import StringType, StructField, StructType

S3_ENDPOINT = "http://minio:9000"
S3_ACCESS_KEY = "minioadmin"
S3_SECRET_KEY = "minioadmin123"

spark = (
    SparkSession.builder
    .appName("velib-bronze-streaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .config("spark.sql.shuffle.partitions", "4")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

envelope_schema = StructType([
    StructField("ingested_at", StringType()),
    StructField("payload", StringType()),
])

def write_stream_to_delta(topic, table_name):
    df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", "kafka:9092")
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .load()
    )
    parsed = (
        df.select(
            from_json(col("value").cast("string"), envelope_schema).alias("env"),
            col("timestamp").alias("kafka_ts"),
        )
        .select(
            col("env.ingested_at").alias("ingested_at"),
            col("env.payload").alias("payload_raw"),
            col("kafka_ts"),
            current_timestamp().alias("bronze_ts"),
        )
    )
    return (
        parsed.writeStream
        .format("delta")
        .option("checkpointLocation", f"s3a://checkpoints/{table_name}")
        .outputMode("append")
        .trigger(processingTime="60 seconds")
        .start(f"s3a://bronze/{table_name}")
    )

q_info = write_stream_to_delta("velib.station_information", "station_information")
q_status = write_stream_to_delta("velib.station_status", "station_status")

spark.streams.awaitAnyTermination()
