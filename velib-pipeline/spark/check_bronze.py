from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("check-bronze")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

for table in ["station_information", "station_status"]:
    df = spark.read.format("delta").load(f"s3a://bronze/{table}")
    print(f"\n===== {table} =====")
    print(f"Nombre de lignes : {df.count()}")
    print(f"Colonnes         : {df.columns}")
    df.select("ingested_at", "bronze_ts").show(3, truncate=False)

spark.stop()
