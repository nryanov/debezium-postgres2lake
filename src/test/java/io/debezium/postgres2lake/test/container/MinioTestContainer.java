package io.debezium.postgres2lake.test.container;

import org.testcontainers.containers.MinIOContainer;
import org.testcontainers.utility.DockerImageName;

public final class MinioTestContainer {

    public static final String ACCESS_KEY = "admin";
    public static final String SECRET_KEY = "password";

    private static final DockerImageName IMAGE = DockerImageName
            .parse("minio/minio:RELEASE.2025-02-28T09-55-16Z")
            .asCompatibleSubstituteFor("minio");

    private static MinIOContainer shared;

    private MinioTestContainer() {
    }

    public static MinIOContainer newDedicated() {
        return new MinIOContainer(IMAGE)
                .withUserName(ACCESS_KEY)
                .withPassword(SECRET_KEY);
    }

    public static synchronized MinIOContainer ensureSharedStarted() {
        if (shared == null) {
            shared = newDedicated();
            shared.start();
        }
        return shared;
    }

    public static void stopShared() {
        if (shared != null) {
            shared.stop();
            shared = null;
        }
    }
}
