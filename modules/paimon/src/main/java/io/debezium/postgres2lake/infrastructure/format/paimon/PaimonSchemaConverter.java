package io.debezium.postgres2lake.infrastructure.format.paimon;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowType;

import static io.debezium.postgres2lake.domain.AvroUtils.unwrapUnion;

public class PaimonSchemaConverter implements SchemaConverter<Schema> {
    @Override
    public Schema extractSchema(EventRecord event) {
        return avroToPaimonSchema(event.keySchema(), event.valueSchema());
    }

    private Schema avroToPaimonSchema(org.apache.avro.Schema avroKeySchema, org.apache.avro.Schema avroValueSchema) {
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

        // TODO: add partition keys
        builder.primaryKey(primaryKeyFields);

        return builder.build();
    }

    public DataType convertAvroSchema(org.apache.avro.Schema avroValueSchema) {
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

}
