package io.debezium.postgres2lake.test;

import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.quarkus.test.common.QuarkusTestResourceLifecycleManager;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Map;

public class MinioResource implements QuarkusTestResourceLifecycleManager {
    private static GenericContainer<?> minio;
    private final static String ACCESS_KEY = "admin";
    private final static String SECRET_ACCESS_KEY = "password";

    private MinioHelper minioHelper;

    @Override
    public void init(Map<String, String> initArgs) {
        QuarkusTestResourceLifecycleManager.super.init(initArgs);
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

        return Map.of();
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
