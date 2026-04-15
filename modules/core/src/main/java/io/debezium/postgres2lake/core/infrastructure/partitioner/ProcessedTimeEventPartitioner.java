package io.debezium.postgres2lake.core.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.time.Clock;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class ProcessedTimeEventPartitioner implements EventPartitioner {
    private final static DateTimeFormatter ISO_LOCAL_DATE = DateTimeFormatter.ISO_LOCAL_DATE.withZone(ZoneOffset.UTC);

    private final Clock clock;

    public ProcessedTimeEventPartitioner() {
        this(Clock.systemUTC());
    }

    public ProcessedTimeEventPartitioner(Clock clock) {
        this.clock = clock;
    }

    @Override
    public String resolvePartition(String rootPath, EventRecord record) {
        var destination = record.destination();
        var now = clock.instant();

        return String.format(
                "%s/%s/%s/%s/%s",
                rootPath,
                destination.database(),
                destination.schema(),
                destination.table(),
                ISO_LOCAL_DATE.format(now));
    }
}
