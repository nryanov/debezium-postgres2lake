package io.debezium.postgres2lake.core.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.EventPartitioner;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.util.Utf8;

public final class RecordFieldEventPartitioner implements EventPartitioner {

    private final String fieldName;

    public RecordFieldEventPartitioner(String fieldName) {
        this.fieldName = fieldName;
    }

    @Override
    public String resolvePartition(String bucket, EventRecord record) {
        if (!record.value().hasField(fieldName)) {
            throw new IllegalArgumentException(String.format("Partition field `%s` not found in value", fieldName));
        }

        var raw = record.value().get(fieldName);

        if (raw == null) {
            throw new IllegalArgumentException("Partition field '%s' is null in event for %s"
                    .formatted(fieldName, record.rawDestination()));
        }

        var segment = switch (raw) {
            case Utf8 u -> u.toString();
            case CharSequence cs -> cs.toString();
            default -> raw.toString();
        };

        if (segment.isEmpty() || segment.contains("/")) {
            throw new IllegalArgumentException("Invalid partition segment for field '%s': %s"
                    .formatted(fieldName, segment));
        }

        var destination = record.destination();
        return String.format(
                "s3a://%s/%s/%s/%s/%s",
                bucket,
                destination.database(),
                destination.schema(),
                destination.table(),
                segment);
    }
}
