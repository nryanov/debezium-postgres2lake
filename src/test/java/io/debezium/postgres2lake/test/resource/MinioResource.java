package io.debezium.postgres2lake.test.resource;

import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class MinioResource implements QuarkusTestResourceLifecycleManager {
    public static final String BUCKET_NAME_ARG = "bucket";
    public static final String FORMAT_TYPE_ARG = "format";

    private static MinIOContainer minio;
    private final static String ACCESS_KEY = "admin";
    private final static String SECRET_ACCESS_KEY = "password";

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
        if (minio == null) {
            minio = new MinIOContainer(
                    DockerImageName.
                            parse("minio/minio:RELEASE.2025-02-28T09-55-16Z")
                            .asCompatibleSubstituteFor("minio")
            ).withUserName(ACCESS_KEY).withPassword(SECRET_ACCESS_KEY);
            minio.start();
        }

        var endpoint = minio.getS3URL();

        minioHelper = new MinioHelper(endpoint, ACCESS_KEY, SECRET_ACCESS_KEY);
        minioHelper.createBucket(bucket);

        var properties = new HashMap<String, String>();
        switch (format) {
            case "avro" -> {
                properties.put("output.avro.file-io.properties.fs.s3a.access.key", ACCESS_KEY);
                properties.put("output.avro.file-io.properties.fs.s3a.secret.key", SECRET_ACCESS_KEY);
                properties.put("output.avro.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("output.avro.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("output.avro.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "parquet" -> {
                properties.put("output.parquet.file-io.properties.fs.s3a.access.key", ACCESS_KEY);
                properties.put("output.parquet.file-io.properties.fs.s3a.secret.key", SECRET_ACCESS_KEY);
                properties.put("output.parquet.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("output.parquet.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("output.parquet.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "orc" -> {
                properties.put("output.orc.file-io.properties.fs.s3a.access.key", ACCESS_KEY);
                properties.put("output.orc.file-io.properties.fs.s3a.secret.key", SECRET_ACCESS_KEY);
                properties.put("output.orc.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("output.orc.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("output.orc.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            case "iceberg" -> {
                properties.put("output.iceberg.properties.io-impl", "org.apache.iceberg.aws.s3.S3FileIO");
                properties.put("output.iceberg.properties.warehouse", String.format("s3a://%s", bucket));
                properties.put("output.iceberg.properties.s3.endpoint", endpoint);
                properties.put("output.iceberg.properties.s3.access-key-id", ACCESS_KEY);
                properties.put("output.iceberg.properties.s3.secret-access-key", SECRET_ACCESS_KEY);
                properties.put("output.iceberg.properties.s3.path-style-access", "true");
                properties.put("output.iceberg.properties.s3.client-factory-impl", "io.debezium.postgres2lake.infrastructure.format.iceberg.InstrumentedS3FileIOAwsClientFactory");
            }
            case "paimon" -> {
                properties.put("output.paimon.properties.warehouse", String.format("s3a://%s", bucket));
                properties.put("output.paimon.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
                properties.put("output.paimon.file-io.properties.fs.s3a.access.key", ACCESS_KEY);
                properties.put("output.paimon.file-io.properties.fs.s3a.secret.key", SECRET_ACCESS_KEY);
                properties.put("output.paimon.file-io.properties.fs.s3a.path.style.access", "true");
                properties.put("output.paimon.file-io.properties.fs.s3a.endpoint", endpoint);
            }
            default -> {}
        }

        return properties;
    }

    @Override
    public void stop() {
        minio.stop();
    }

    @Override
    public void inject(TestInjector testInjector) {
        testInjector.injectIntoFields(minioHelper, new TestInjector.AnnotatedAndMatchesType(InjectMinioHelper.class, MinioHelper.class));
    }
}
