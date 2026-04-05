package io.debezium.postgres2lake.infrastructure.partitioner;

import org.junit.jupiter.api.Test;

import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ProcessedTimeEventPartitionerTest {

    @Test
    void resolvePartition_usesClockInstantAsUtcDateSegment() {
        var clock = Clock.fixed(Instant.parse("2024-06-15T18:30:00Z"), ZoneOffset.UTC);
        var partitioner = new ProcessedTimeEventPartitioner(clock);
        var record = EventRecordTestSupport.recordWithEventTime(0L);

        assertEquals(
                "s3a://bucket/db/schema/table/2024-06-15",
                partitioner.resolvePartition("bucket", record));
    }
}
