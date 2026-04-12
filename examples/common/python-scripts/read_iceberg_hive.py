from pyspark.sql import SparkSession

HIVE_METASTORE_URI = "thrift://hive-metastore:9083"
WAREHOUSE = "s3a://warehouse/hive-warehouse"

S3A_CONF = {
    "spark.hadoop.fs.s3a.endpoint": "http://minio:9000",
    "spark.hadoop.fs.s3a.access.key": "admin",
    "spark.hadoop.fs.s3a.secret.key": "password",
    "spark.hadoop.fs.s3a.path.style.access": "true",
    "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.hadoop.fs.s3a.region": "none",
}

spark = (
    SparkSession.builder.appName("read-iceberg-hive")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.iceberg.type", "hive")
    .config("spark.sql.catalog.iceberg.uri", HIVE_METASTORE_URI)
    .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
    .config("spark.sql.catalog.iceberg.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.iceberg.s3.endpoint", "http://minio:9000")
    .config("spark.sql.catalog.iceberg.s3.access-key-id", "admin")
    .config("spark.sql.catalog.iceberg.s3.secret-access-key", "password")
    .config("spark.sql.catalog.iceberg.s3.path-style-access", "true")
    .config("spark.sql.catalog.iceberg.s3.region", "none")
    .config("spark.jars", "/home/jovyan/extra-jars/iceberg-spark-runtime-4.0_2.13-1.10.1.jar")
)
for k, v in S3A_CONF.items():
    spark = spark.config(k, v)

spark = spark.getOrCreate()

database = "default_public"
table = "demo_orders"

df = spark.sql(f"SELECT * FROM iceberg.{database}.{table}")
df.show(20, truncate=False)

spark.stop()
