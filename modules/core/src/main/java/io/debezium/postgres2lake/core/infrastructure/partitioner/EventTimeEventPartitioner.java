package io.debezium.postgres2lake.core.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class EventTimeEventPartitioner implements EventPartitioner {
    private final static DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    @Override
    public String resolvePartition(String rootPath, EventRecord record) {
        var destination = record.destination();
        var eventTime = record.eventTime();
        var partition = ISO_LOCAL_DATE.format(Instant.ofEpochMilli(eventTime));

        return String.format(
                "%s/%s/%s/%s/%s",
                rootPath,
                destination.database(),
                destination.schema(),
                destination.table(),
                partition);
    }
}
