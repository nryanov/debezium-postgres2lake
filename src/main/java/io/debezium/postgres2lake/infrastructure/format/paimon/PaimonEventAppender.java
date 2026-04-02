package io.debezium.postgres2lake.infrastructure.format.paimon;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.generic.GenericRecord;
import org.apache.paimon.data.BinaryString;
import org.apache.paimon.data.Decimal;
import org.apache.paimon.data.GenericArray;
import org.apache.paimon.data.GenericMap;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.data.Timestamp;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.RowKind;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.convertToBytes;
import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.convertToString;
import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.unwrapUnion;

public class PaimonEventAppender implements EventAppender<PaimonTableWriter> {
    @Override
    public void appendEvent(EventRecord event, PaimonTableWriter writer) throws Exception {
        var write = writer.writer().get();
        if (write == null) {
            write = writer.writeBuilder().newWrite();
            writer.writer().set(write);
        }

        // todo: fix bucket id resolution
        var bucket = 0;
        write.write(createPaimonRecord(writer.paimonSchema(), event), bucket);
    }

    @Override
    public void commitPendingEvents(PaimonTableWriter writer) throws Exception {
        var write = writer.writer().get();
        var pendingCommit = write.prepareCommit(false, writer.commitId().incrementAndGet());
        writer.pendingCommits().addAll(pendingCommit);

        var commit = writer.writeBuilder().newCommit();
        commit.commit(writer.commitId().get(), writer.pendingCommits());
        writer.pendingCommits().clear();
    }

    private GenericRow createPaimonRecord(Schema paimonSchema, EventRecord event) {
        var arity = paimonSchema.fields().size();
        var row = new GenericRow(arity);

        var avroRecord = event.value();
        var avroSchema = avroRecord.getSchema();
        var idx = 0;

        for (var avroField : avroSchema.getFields()) {
            var avroValue = avroRecord.get(avroField.name());
            row.setField(idx, convertAvroToPaimonValue(avroField.schema(), avroValue));
            idx++;
        }

        var kind = switch (event.operation()) {
            case INSERT -> RowKind.INSERT;
            case UPDATE -> RowKind.UPDATE_AFTER;
            case DELETE -> RowKind.DELETE;
        };
        row.setRowKind(kind);

        return row;
    }

    /**
     * {@link org.apache.paimon.data.InternalRow}
     */
    private Object convertAvroToPaimonValue(org.apache.avro.Schema avroSchema, Object avroValue) {
        if (avroValue == null) {
            return null;
        }

        var logicalType = avroSchema.getLogicalType();
        if (logicalType != null) {
            return convertLogicalAvroToPaimonValue(logicalType, avroValue);
        }

        return switch (avroSchema.getType()) {
            case STRING, ENUM -> BinaryString.fromString(convertToString(avroValue));
            case BOOLEAN, FLOAT, DOUBLE, INT, LONG, FIXED, BYTES -> avroValue;
            case UNION -> convertAvroToPaimonValue(unwrapUnion(avroSchema), avroValue);
            case ARRAY -> {
                var rawArray = (List<?>) avroValue;
                var array = new Object[rawArray.size()];
                var elementType = avroSchema.getElementType();

                var idx = 0;

                for (var el : rawArray) {
                    array[idx] = convertAvroToPaimonValue(elementType, el);
                    idx++;
                }

                yield new GenericArray(array);
            }
            case MAP -> {
                var rawMap = (Map<?, ?>) avroValue;
                var map = new HashMap<>();

                rawMap.forEach((key, value) -> {
                    map.put(key, convertAvroToPaimonValue(avroSchema.getValueType(), value));
                });

                yield new GenericMap(map);
            }
            case RECORD -> {
                var record = (GenericRecord) avroValue;
                var arity = avroSchema.getFields().size();
                var innerRecord = new GenericRow(arity);
                var idx = 0;

                for (var avroField : avroSchema.getFields()) {
                    var innerValue = record.get(avroField.name());
                    innerRecord.setField(idx, convertAvroToPaimonValue(avroField.schema(), innerValue));
                    idx++;
                }

                yield innerRecord;
            }
            default -> throw new IllegalArgumentException("Unsupported type: " + avroSchema);
        };
    }

    private Object convertLogicalAvroToPaimonValue(LogicalType logicalType, Object avroValue) {
        return switch (logicalType) {
            case LogicalTypes.Decimal type -> {
                var bytes = convertToBytes(avroValue);
                yield Decimal.fromUnscaledBytes(bytes, type.getPrecision(), type.getScale());
            }
            case LogicalTypes.Uuid ignored -> BinaryString.fromString(convertToString(avroValue));
            case LogicalTypes.TimeMicros ignored -> (int) (((Number) avroValue).longValue() / 1_000L); // paimon expects number of millis of the day
            case LogicalTypes.TimeMillis ignored -> (int) avroValue;
            case LogicalTypes.TimestampMicros ignored -> Timestamp.fromMicros((long) avroValue);
            case LogicalTypes.TimestampMillis ignored -> Timestamp.fromEpochMillis((long) avroValue);
            case LogicalTypes.LocalTimestampMicros ignored -> Timestamp.fromMicros((long) avroValue);
            case LogicalTypes.LocalTimestampMillis ignored -> Timestamp.fromEpochMillis((long) avroValue);
            case LogicalTypes.Date ignored -> (int) avroValue;
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
    }
}
