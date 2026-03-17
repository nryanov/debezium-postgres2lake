package io.debezium.postgres2lake.infrastructure.format.paimon;

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
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.apache.paimon.types.RowType;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.*;

public class AvroToPaimonMapper {
    public Schema avroToPaimonSchema(org.apache.avro.Schema avroKeySchema, org.apache.avro.Schema avroValueSchema) {
        var paimonRow = (RowType) convertAvroSchema(avroValueSchema);
        var builder = Schema.newBuilder();

        for (var field : paimonRow.getFields()) {
            builder.column(field.name(), field.type());
        }

        var primaryKeyFields = new String[avroKeySchema.getFields().size()];
        var idx = 0;

        for (var field : avroKeySchema.getFields()) {
            primaryKeyFields[idx] = field.name();
            idx++;
        }

        builder.primaryKey(primaryKeyFields);

        return builder.build();
    }

    private DataType convertAvroSchema(org.apache.avro.Schema avroValueSchema) {
        var logicalType = avroValueSchema.getLogicalType();
        if (logicalType != null) {
            return convertLogicalAvroType(logicalType, avroValueSchema);
        }

        return switch (avroValueSchema.getType()) {
            case RECORD -> {
                var avroFields = avroValueSchema.getFields();
                var paimonFields = new DataField[avroFields.size()];
                var idx = 0;

                for (var field : avroFields) {
                    var paimonField = DataTypes.FIELD(idx, field.name(), convertAvroSchema(field.schema()), field.doc());
                    paimonFields[idx] = paimonField;
                    idx++;
                }

                yield DataTypes.ROW(paimonFields);
            }
            case UNION -> convertAvroSchema(unwrapUnion(avroValueSchema)).nullable();
            case MAP -> DataTypes.MAP(DataTypes.STRING(), convertAvroSchema(avroValueSchema.getValueType()));
            case ARRAY -> DataTypes.ARRAY(convertAvroSchema(avroValueSchema.getElementType()));
            case BYTES -> DataTypes.BYTES();
            case FIXED -> DataTypes.BINARY(avroValueSchema.getFixedSize());
            case INT -> DataTypes.INT();
            case LONG -> DataTypes.BIGINT();
            case FLOAT -> DataTypes.FLOAT();
            case DOUBLE -> DataTypes.DOUBLE();
            case STRING, ENUM -> DataTypes.STRING();
            case BOOLEAN -> DataTypes.BOOLEAN();
            default -> throw new IllegalArgumentException("Unsupported type: " + avroValueSchema);
        };
    }

    private DataType convertLogicalAvroType(LogicalType logicalType, org.apache.avro.Schema avroValueSchema) {
        return switch (logicalType) {
            case LogicalTypes.Decimal type -> DataTypes.DECIMAL(type.getPrecision(), type.getScale());
            case LogicalTypes.Uuid ignored -> DataTypes.STRING();
            case LogicalTypes.TimeMicros ignored -> DataTypes.TIME(3);
            case LogicalTypes.TimeMillis ignored -> DataTypes.TIME(3);
            case LogicalTypes.TimestampMicros ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield DataTypes.TIMESTAMP(6);
                } else {
                    yield DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
                }
            }
            case LogicalTypes.TimestampMillis ignored -> {
                var adjustToUtc = (boolean) avroValueSchema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield DataTypes.TIMESTAMP(3);
                } else {
                    yield DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
                }
            }
            case LogicalTypes.LocalTimestampMicros ignored -> DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(6);
            case LogicalTypes.LocalTimestampMillis ignored -> DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE(3);
            case LogicalTypes.Date ignored -> DataTypes.DATE();
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
    }

    public GenericRow createPaimonRecord(Schema paimonSchema, EventRecord event) {
        var arity = paimonSchema.fields().size();
        var row = new GenericRow(arity);

        var avroRecord = event.value();

        if (avroRecord != null) {
            var avroSchema = avroRecord.getSchema();
            var idx = 0;

            for (var avroField : avroSchema.getFields()) {
                var avroValue = avroRecord.get(avroField.name());
                row.setField(idx, convertAvroToPaimonValue(avroField.schema(), avroValue));
                idx++;
            }
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
