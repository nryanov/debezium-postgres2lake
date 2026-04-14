package io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg;

import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventDestination;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.MathContext;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.postgres2lake.domain.AvroUtils.convertToBytes;
import static io.debezium.postgres2lake.domain.AvroUtils.convertToString;
import static io.debezium.postgres2lake.domain.AvroUtils.convertToUuid;

public class IcebergEventAppender implements EventAppender {
    private final IcebergTableWriter writer;

    public IcebergEventAppender(IcebergTableWriter writer) {
        this.writer = writer;
    }

    @Override
    public void appendEvent(EventRecord event) throws IOException {
        var record = createIcebergRecord(writer.icebergSchema(), event.value());
        writer.writer().write(new EnrichedRecordWrapper(record, event.operation()));
    }

    @Override
    public void commitPendingEvents() throws Exception {
        var rs = writer.writer().complete();

        if (rs.deleteFiles().length > 0) {
            var delta = writer.table().newRowDelta();
            var dataFiles = rs.dataFiles();
            var deleteFiles = rs.deleteFiles();
            Arrays.stream(dataFiles).forEach(delta::addRows);
            Arrays.stream(deleteFiles).forEach(delta::addDeletes);
            delta.commit();
        } else {
            var dataFiles = rs.dataFiles();
            var appendIo = writer.table().newAppend();
            Arrays.stream(dataFiles).forEach(appendIo::appendFile);
            appendIo.commit();
        }
    }

    @Override
    public String currentPartition() {
        // partition is resolved via iceberg fileIO
        return "";
    }

    @Override
    public org.apache.avro.Schema currentSchema() {
        return writer.schema();
    }

    @Override
    public EventDestination destination() {
        return writer.destination();
    }

    public Table table() {
        return writer.table();
    }

    private Record createIcebergRecord(Schema icebergSchema, org.apache.avro.generic.GenericRecord avroRecord) {
        var icebergRecord = GenericRecord.create(icebergSchema);
        var fields = avroRecord.getSchema().getFields();

        for (var field : fields) {
            var icebergField = icebergSchema.findField(field.name());
            var value = toIcebergValue(icebergField, field.schema(), avroRecord.get(field.name()));
            icebergRecord.setField(field.name(), value);
        }

        return icebergRecord;
    }

    private Object toIcebergValue(Types.NestedField icebergField, org.apache.avro.Schema avroSchema, Object avroValue) {
        if (avroValue == null) {
            return null;
        }

        return switch (avroSchema.getType()) {
            case ENUM -> convertToString(avroValue);
            case STRING -> {
                var logicalType = avroSchema.getLogicalType();
                if (logicalType != null) {
                    if (logicalType instanceof LogicalTypes.Uuid) {
                        yield convertToUuid(avroValue);
                    }
                }

                yield convertToString(avroValue);
            }
            case BOOLEAN, FIXED, DOUBLE, FLOAT -> avroValue;
            case INT -> {
                var number = (Number) avroValue;
                var logicalType = avroSchema.getLogicalType();
                if (logicalType != null) {
                    if (logicalType instanceof LogicalTypes.Date) {
                        yield LocalDate.ofEpochDay(number.intValue());
                    }

                    if (logicalType instanceof LogicalTypes.TimeMillis) {
                        yield Instant.ofEpochMilli(number.intValue())
                                .atZone(ZoneOffset.UTC)
                                .toLocalTime();
                    }
                }

                yield avroValue;
            }
            case LONG -> {
                var number = (Number) avroValue;
                var logicalType = avroSchema.getLogicalType();
                if (logicalType != null) {
                    switch (logicalType) {
                        case LogicalTypes.TimestampMicros ignored -> {
                            var micros = number.longValue();
                            var seconds = micros / 1_000_000;
                            var microsAndNanos = (micros % 1_000) * 1_000;

                            var adjustToUtc = (boolean) avroSchema.getObjectProp("adjust-to-utc");
                            if (adjustToUtc) {
                                yield OffsetDateTime.ofInstant(Instant.ofEpochSecond(seconds, microsAndNanos), ZoneOffset.UTC.normalized());
                            } else {
                                yield LocalDateTime.ofEpochSecond((int) seconds, (int) microsAndNanos, ZoneOffset.UTC);
                            }
                        }
                        // todo: fix avro versions. Currently this logical type is ignored
                        case LogicalTypes.LocalTimestampMicros ignored -> {
                            var micros = number.longValue();
                            var seconds = micros / 1_000_000;
                            var microsAndNanos = (micros % 1_000) * 1_000;

                            yield LocalDateTime.ofEpochSecond((int) seconds, (int) microsAndNanos, ZoneOffset.UTC);
                        }
                        case LogicalTypes.TimeMicros ignored -> {
                            yield LocalTime.ofNanoOfDay(number.longValue() * 1_000L);
                        }
                        default -> {}
                    }

                }

                yield avroValue;
            }
            case BYTES -> {
                var logicalType = avroSchema.getLogicalType();
                if (logicalType != null) {
                    var decimalType = (LogicalTypes.Decimal) logicalType;
                    var bytes = convertToBytes(avroValue);
                    yield  new BigDecimal(new BigInteger(bytes), decimalType.getScale(), new MathContext(decimalType.getPrecision(), RoundingMode.HALF_UP));
                }

                yield avroValue;
            }
            case ARRAY -> saveArray(icebergField, avroSchema, avroValue);
            case MAP -> saveMap(icebergField, avroSchema, avroValue);
            case UNION -> saveUnion(icebergField, avroSchema, avroValue);
            case RECORD -> saveInnerRecord(icebergField, avroSchema, avroValue);
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    private Object saveMap(Types.NestedField icebergField, org.apache.avro.Schema avroSchema, Object avroValue) {
        var rawMap = (Map<?, ?>) avroValue;
        var mappedMap = new HashMap<>();

        rawMap.forEach((key, value) -> {
            mappedMap.put(key, toIcebergValue(icebergField, avroSchema.getValueType(), value));
        });

        return mappedMap;
    }

    private Object saveArray(Types.NestedField icebergField, org.apache.avro.Schema avroSchema, Object avroValue) {
        var rawArray = (List<?>) avroValue;
        var mappedArray = new ArrayList<>();

        rawArray.forEach(it -> mappedArray.add(toIcebergValue(icebergField, avroSchema.getElementType(), it)));

        return mappedArray;
    }

    private Object saveInnerRecord(Types.NestedField icebergField, org.apache.avro.Schema avroSchema, Object avroValue) {
        var icebergSchema = icebergField.type().asStructType().asSchema();
        var innerRecord = GenericRecord.create(icebergField.type().asStructType());

        var avroRecord = (org.apache.avro.generic.GenericRecord) avroValue;
        var avroFields = avroSchema.getFields();

        for (var field : avroFields) {
            var innerIcebergField = icebergSchema.findField(field.name());
            var value = toIcebergValue(innerIcebergField, field.schema(), avroRecord.get(field.name()));
            innerRecord.setField(field.name(), value);
        }

        return innerRecord;
    }

    private Object saveUnion(Types.NestedField icebergField, org.apache.avro.Schema avroSchema, Object avroValue) {
        for (org.apache.avro.Schema s : avroSchema.getTypes()) {
            if (s.getType() != org.apache.avro.Schema.Type.NULL) {
                return toIcebergValue(icebergField, s, avroValue);
            }
        }

        throw new IllegalArgumentException("Incorrect union type");
    }
}
