package io.debezium.postgres2lake.orc.infrastructure;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.LogicalType;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.orc.TypeDescription;

public class OrcSchemaConverter implements SchemaConverter<TypeDescription> {
    @Override
    public TypeDescription extractSchema(EventRecord event) {
        return avroToOrcSchema(event.valueSchema());
    }

    private TypeDescription avroToOrcSchema(Schema schema) {
        var logicalType = schema.getLogicalType();
        if (logicalType != null) {
            return avroLogicalToOrcSchema(logicalType, schema);
        }

        return switch (schema.getType()) {
            case INT -> TypeDescription.createInt();
            case STRING, ENUM -> TypeDescription.createString();
            case BOOLEAN -> TypeDescription.createBoolean();
            case LONG -> TypeDescription.createLong();
            case FLOAT -> TypeDescription.createFloat();
            case DOUBLE -> TypeDescription.createDouble();
            case FIXED, BYTES -> TypeDescription.createBinary();
            case UNION -> {
                // use first not null paimonSchema
                if (schema.getType() == Schema.Type.UNION) {
                    for (Schema s : schema.getTypes()) {
                        if (s.getType() != Schema.Type.NULL) yield avroToOrcSchema(s);
                    }
                }

                throw new IllegalArgumentException("Unsupported type");
            }
            case MAP -> TypeDescription.createMap(TypeDescription.createString(), avroToOrcSchema(schema.getValueType()));
            case ARRAY -> TypeDescription.createList(avroToOrcSchema(schema.getElementType()));
            case RECORD -> {
                var struct = TypeDescription.createStruct();
                for (var field : schema.getFields()) {
                    struct.addField(field.name(), avroToOrcSchema(field.schema()));
                }
                yield struct;
            }
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    private TypeDescription avroLogicalToOrcSchema(LogicalType logicalType, Schema schema) {
        return switch (logicalType) {
            case LogicalTypes.Decimal decimal -> TypeDescription
                    .createDecimal()
                    .withScale(decimal.getScale())
                    .withPrecision(decimal.getPrecision());
            case LogicalTypes.Uuid ignored ->  TypeDescription.createString();
            case LogicalTypes.TimeMicros ignored -> TypeDescription.createLong();
            case LogicalTypes.TimeMillis ignored -> TypeDescription.createInt();
            case LogicalTypes.TimestampMicros ignored -> {
                var adjustToUtc = (boolean) schema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield TypeDescription.createTimestamp();
                } else {
                    yield TypeDescription.createTimestampInstant();
                }
            }
            case LogicalTypes.TimestampMillis ignored -> {
                var adjustToUtc = (boolean) schema.getObjectProp("adjust-to-utc");

                if (adjustToUtc) {
                    yield TypeDescription.createTimestamp();
                } else {
                    yield TypeDescription.createTimestampInstant();
                }
            }
            case LogicalTypes.LocalTimestampMicros ignored -> TypeDescription.createTimestampInstant();
            case LogicalTypes.LocalTimestampMillis ignored -> TypeDescription.createTimestampInstant();
            case LogicalTypes.Date ignored -> TypeDescription.createDate();
            default -> throw new IllegalArgumentException("Unsupported logical type: " + logicalType);
        };
    }
}
