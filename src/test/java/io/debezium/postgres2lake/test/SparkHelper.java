package io.debezium.postgres2lake.test;

import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

/**
 * Spark reads against MinIO (S3A) for integration test assertions.
 */
public final class SparkHelper {
    private final SparkSession spark;

    public SparkHelper(PostgresHelper postgresHelper, MinioHelper minioHelper) {
        var jdbcUrl = postgresHelper.jdbcUrl();
        var minioEndpoint = minioHelper.endpoint();

        this.spark = SparkSession.builder()
                .appName("postgres2lake-it")
                .master("local[1]")
                .config("spark.ui.enabled", false)
                .config("spark.sql.shuffle.partitions", "2")
                .config("spark.hadoop.fs.s3a.endpoint", minioEndpoint)
                .config("spark.hadoop.fs.s3a.access.key", "admin")
                .config("spark.hadoop.fs.s3a.secret.key", "password")
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                .config("spark.sql.catalog.iceberg_catalog", "org.apache.iceberg.spark.SparkCatalog")
                .config("spark.sql.catalog.iceberg_catalog.type", "jdbc")
                .config("spark.sql.catalog.iceberg_catalog.uri", jdbcUrl)
                .config("spark.sql.catalog.iceberg_catalog.jdbc.user", "postgres")
                .config("spark.sql.catalog.iceberg_catalog.jdbc.password", "postgres")
                .config("spark.sql.catalog.iceberg_catalog.jdbc.schema-version", "V1")
                .config("spark.sql.catalog.iceberg_catalog.warehouse", "s3a://warehouse")
                .config("spark.sql.catalog.iceberg_catalog.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
                .config("spark.sql.catalog.iceberg_catalog.s3.endpoint", minioEndpoint)
                .config("spark.sql.catalog.iceberg_catalog.s3.access-key-id", "admin")
                .config("spark.sql.catalog.iceberg_catalog.s3.secret-access-key", "password")
                .config("spark.sql.catalog.iceberg_catalog.s3.path-style-access", "true")
                .config("spark.sql.catalog.iceberg_catalog.s3.client-factory-impl",
                        "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory")
                .config("spark.sql.catalog.paimon", "org.apache.paimon.spark.SparkCatalog")
                .config("spark.sql.catalog.paimon.warehouse", "s3a://warehouse/paimon-warehouse")
                .config("spark.sql.catalog.paimon.option.type", "jdbc")
                .config("spark.sql.catalog.paimon.option.jdbc-url", jdbcUrl)
                .config("spark.sql.catalog.paimon.option.jdbc-user", "postgres")
                .config("spark.sql.catalog.paimon.option.jdbc-password", "postgres")
                .config("spark.sql.catalog.paimon.option.jdbc-driver", "org.postgresql.Driver")
                .config("spark.sql.catalog.paimon.option.jdbc-table-prefix", "paimon_")
                .getOrCreate();
    }

    public void show(String format, String path) {
        var df = spark.read().format(format).load(path);
        df.show();
    }

    public long countFileRowsWithPk(String format, String path, long primaryKey) {
        var df = spark.read().format(format).load(path);
        return df.filter(col("primary_key").equalTo(lit(primaryKey))).count();
    }

    public long countIcebergRowsWithPk(long primaryKey) {
        return spark.sql(
                "SELECT count(*) AS c FROM iceberg_catalog.development.`data` WHERE primary_key = " + primaryKey
        ).first().getLong(0);
    }

    public long countPaimonRowsWithPk(long primaryKey) {
        return spark.sql(
                "SELECT count(*) AS c FROM paimon.`paimon-development`.`data` WHERE primary_key = " + primaryKey
        ).first().getLong(0);
    }
}
