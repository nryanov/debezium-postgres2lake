package io.debezium.postgres2lake.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ProcessedTimeEventPartitioner implements EventPartitioner {
    private final static DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    @Override
    public String resolvePartition(String bucket, EventRecord record) {
        var destination = record.destination();
        var now = Instant.now();

        return String.format("s3a://%s/%s/%s/%s/%s", bucket, destination.database(), destination.schema(), destination.table(), ISO_LOCAL_DATE.format(now));
    }
}
