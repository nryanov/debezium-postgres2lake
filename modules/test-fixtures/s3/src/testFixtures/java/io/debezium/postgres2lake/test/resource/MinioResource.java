package io.debezium.postgres2lake.test.resource;

import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.container.MinioTestContainer;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;

import java.util.HashMap;
import java.util.Map;

public class MinioResource implements QuarkusTestResourceLifecycleManager {
    public static final String BUCKET_NAME_ARG = "bucket";
    public static final String FORMAT_TYPE_ARG = "format";

    private MinioHelper minioHelper;
    private String bucket;
    private String format;

    @Override
    public void init(Map<String, String> initArgs) {
        this.bucket = initArgs.get(BUCKET_NAME_ARG);
        this.format = initArgs.get(FORMAT_TYPE_ARG);

        if (bucket == null || format == null) {
            throw new IllegalArgumentException("Bucket name and format type should be passed as initArg in MinioResource");
        }
    }

    @Override
    public Map<String, String> start() {
        var minio = MinioTestContainer.ensureSharedStarted();
        var endpoint = minio.getS3URL();

        minioHelper = new MinioHelper(endpoint, MinioTestContainer.ACCESS_KEY, MinioTestContainer.SECRET_KEY);
        minioHelper.createBucket(bucket);

        var properties = new HashMap<String, String>();
        switch (format) {
            case "avro" -> {
                properties.put("debezium.output.avro.file-io.properties.fs.s3a.access.key", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.avro.file-io.properties.fs.s3a.secret.key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.avro.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("debezium.output.avro.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("debezium.output.avro.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "parquet" -> {
                properties.put("debezium.output.parquet.file-io.properties.fs.s3a.access.key", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.parquet.file-io.properties.fs.s3a.secret.key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.parquet.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("debezium.output.parquet.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("debezium.output.parquet.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "orc" -> {
                properties.put("debezium.output.orc.file-io.properties.fs.s3a.access.key", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.orc.file-io.properties.fs.s3a.secret.key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.orc.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("debezium.output.orc.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("debezium.output.orc.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "iceberg" -> {
                properties.put("debezium.output.iceberg.properties.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("debezium.output.iceberg.properties.warehouse", String.format("s3a://%s", bucket));
                properties.put("debezium.output.iceberg.properties.s3.endpoint", endpoint);
                properties.put("debezium.output.iceberg.properties.s3.access-key-id", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.iceberg.properties.s3.secret-access-key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.iceberg.properties.s3.path-style-access", "true");
                properties.put("debezium.output.iceberg.properties.s3.client-factory-impl", "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");
            }
            case "iceberg-hadoop" -> {
                properties.put("debezium.output.iceberg.properties.type", "hadoop");
                properties.put("debezium.output.iceberg.properties.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("debezium.output.iceberg.properties.warehouse", String.format("s3a://%s/iceberg-warehouse", bucket));
                properties.put("debezium.output.iceberg.properties.s3.endpoint", endpoint);
                properties.put("debezium.output.iceberg.properties.s3.access-key-id", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.iceberg.properties.s3.secret-access-key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.iceberg.properties.s3.path-style-access", "true");
                properties.put("debezium.output.iceberg.properties.s3.client-factory-impl", "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");
            }
            case "paimon" -> {
                properties.put("debezium.output.paimon.properties.warehouse", String.format("s3a://%s", bucket));
                properties.put("debezium.output.paimon.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("debezium.output.paimon.file-io.properties.fs.s3a.access.key", MinioTestContainer.ACCESS_KEY);
                properties.put("debezium.output.paimon.file-io.properties.fs.s3a.secret.key", MinioTestContainer.SECRET_KEY);
                properties.put("debezium.output.paimon.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("debezium.output.paimon.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            default -> {}
        }

        return properties;
    }

    @Override
    public void stop() {
        MinioTestContainer.stopShared();
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(minioHelper, new TestInjector.AnnotatedAndMatchesType(InjectMinioHelper.class, MinioHelper.class));
    }
}
