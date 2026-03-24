package io.debezium.postgres2lake.test;

import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.HashMap;
import java.util.Map;

public class MinioResource implements QuarkusTestResourceLifecycleManager {
    public static final String BUCKET_NAME_ARG = "bucket";

    private static GenericContainer<?> minio;
    private final static String ACCESS_KEY = "admin";
    private final static String SECRET_ACCESS_KEY = "password";

    private MinioHelper minioHelper;
    private String bucket;

    @Override
    public void init(Map<String, String> initArgs) {
        this.bucket = initArgs.get(BUCKET_NAME_ARG);

        if (bucket == null) {
            throw new IllegalArgumentException("Bucket name should be passed as initArg in MinioResource");
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
        // avro settings
        properties.put("output.avro.file-io.properties.fs.s3a.access.key", ACCESS_KEY);
        properties.put("output.avro.file-io.properties.fs.s3a.secret.key", SECRET_ACCESS_KEY);
        properties.put("output.avro.file-io.properties.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        properties.put("output.avro.file-io.properties.fs.s3a.path.style.access", "true");
        properties.put("output.avro.file-io.properties.fs.s3a.endpoint", endpoint);

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
