package io.debezium.postgres2lake.core.infrastructure.partitioner;

import org.junit.jupiter.api.Test;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class EventTimeEventPartitionerTest {

    @Test
    void resolvePartition_formatsEventTimeAsUtcIsoLocalDate() {
        var partitioner = new EventTimeEventPartitioner();
        var epochMillis = Instant.parse("2023-01-02T00:00:00Z").toEpochMilli();
        var record = EventRecordTestSupport.recordWithEventTime(epochMillis);

        assertEquals(
                "s3a://bucket/db/schema/table/2023-01-02",
                partitioner.resolvePartition("s3a://bucket", record));
    }
}
