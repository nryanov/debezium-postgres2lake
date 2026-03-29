package io.debezium.postgres2lake.test.helper;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.service.AbstractEventSaver;

import java.time.Duration;
import java.util.List;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class WriterRolloverAssertions {

    private WriterRolloverAssertions() {}

    public static void awaitAndFlush(EventSaver eventSaver, AbstractEventSaver<?> saver) {
        awaitNonZeroBacklog(saver);
        eventSaver.flush();
    }

    public static void assertAtLeastTwoDataFiles(MinioHelper minio, String bucket, String keyPrefix, String suffix, String message) {
        var n = countDataFiles(minio, bucket, keyPrefix, suffix);
        assertTrue(n >= 2, message + " (found " + n + " files with suffix " + suffix + ")");
    }

    private static long countDataFiles(MinioHelper minio, String bucket, String keyPrefix, String suffix) {
        var keys = minio.listObjectKeys(bucket, keyPrefix);
        return keys.stream().filter(key -> key.endsWith(suffix)).count();
    }

    private static void awaitNonZeroBacklog(AbstractEventSaver<?> saver) {
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> saver.getCurrentRecords() > 0);
    }
}
