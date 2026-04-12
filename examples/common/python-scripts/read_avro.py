from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("read-s3")
    .master("local[1]")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.region", "none")
    .config("spark.hadoop.fs.s3a.access.key", "admin")
    .config("spark.hadoop.fs.s3a.secret.key", "password")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .getOrCreate()
)

df = spark.read.format("avro").load("s3a://warehouse/default/public/demo_orders/*.avro")

df.show()