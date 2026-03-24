package io.debezium.postgres2lake.test;

import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class MinioResource implements QuarkusTestResourceLifecycleManager {
    public static final String BUCKET_NAME_ARG = "bucket";
    public static final String FORMAT_TYPE_ARG = "format";

    private static GenericContainer<?> minio;
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
        minio = new GenericContainer<>(DockerImageName.parse("minio/minio:RELEASE.2025-02-28T09-55-16Z"))
                .withEnv("MINIO_ROOT_USER", ACCESS_KEY)
                .withEnv("MINIO_ROOT_PASSWORD", SECRET_ACCESS_KEY)
                .withCommand("server", "/data")
                .withExposedPorts(9000);
        minio.start();

        var endpoint = "http://" + minio.getHost() + ":" + minio.getMappedPort(9000);

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
