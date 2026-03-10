package io.debezium.postgres2lake.infrastructure.format.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;

public class AvroToIcebergMapper {
    public Schema avroToIcebergSchema(org.apache.avro.Schema keySchema, org.apache.avro.Schema avroValueSchema) {
        // todo: add information about PK
        return toIcebergSchema(avroValueSchema).asStructType().asSchema();
    }

    private Type toIcebergSchema(org.apache.avro.Schema schema) {
        return switch (schema.getType()) {
            case RECORD -> {
                var nestedFields = new ArrayList<Types.NestedField>();
                for (var field : schema.getFields()) {
                    var type = toIcebergSchema(field.schema());
                    var isOptional = isOptional(field.schema());
                    // todo: generate field ID
                    var nestedField = Types.NestedField.builder().isOptional(isOptional).withDoc(field.doc()).withName(field.name()).withId(0).ofType(type).build();
                    nestedFields.add(nestedField);
                }

                yield Types.StructType.of(nestedFields);
            }
            case UNION -> {
                if (schema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL) {
                    yield toIcebergSchema(schema.getTypes().get(1));
                } else yield toIcebergSchema(schema.getTypes().get(0));
            }
            case ARRAY -> {
                var elementSchema = schema.getElementType();
                // todo: generate field ID
                var id = 0;
                if (AvroSchemaUtil.isOptionSchema(elementSchema)) {
                    yield Types.ListType.ofOptional(id, toIcebergSchema(elementSchema));
                } else {
                    yield Types.ListType.ofRequired(id, toIcebergSchema(elementSchema));
                }
            }
            case MAP -> {
                var valueSchema = schema.getValueType();
                // todo: generate field ID
                int keyId = 0;
                int valueId = 1;

                if (AvroSchemaUtil.isOptional(valueSchema)) {
                    yield Types.MapType.ofOptional(keyId, valueId, Types.StringType.get(), toIcebergSchema(valueSchema));
                } else {
                    yield Types.MapType.ofRequired(keyId, valueId, Types.StringType.get(), toIcebergSchema(valueSchema));
                }
            }
            default -> toIcebergPrimitiveType(schema);
        };
    }

    private boolean isOptional(org.apache.avro.Schema schema) {
        if (schema.getType() == org.apache.avro.Schema.Type.UNION && schema.getTypes().size() == 2) {
            if (schema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL) {
                return true;
            } else return schema.getTypes().get(1).getType() == org.apache.avro.Schema.Type.NULL;
        }
        return false;
    }

    private Type toIcebergPrimitiveType(org.apache.avro.Schema schema) {
        var valueType = schema.getType().getName().toLowerCase();
        var connectLogicalType = schema.getProp("connect.name");
        if (connectLogicalType != null) {
            valueType = connectLogicalType;
        }

        return switch (valueType) {
            // debezium logical type mappings
            case "io.debezium.data.Bits" -> Types.BinaryType.get();
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> {}
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime" -> {}
            case "io.debezium.time.MicroDuration" -> {}
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Enum" -> {}
            case "io.debezium.data.Uuid" -> Types.UUIDType.get();
            case "io.debezium.time.Date" -> {}
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> {}
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp" -> {}
            case "io.debezium.time.IsoDate" -> {}
            case "io.debezium.data.VariableScaleDecimal" -> {}
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> {}
            case "org.apache.kafka.connect.data.Date" -> {}
            case "org.apache.kafka.connect.data.Time" -> {}
            case "org.apache.kafka.connect.data.Timestamp" -> {}
            // avro logical type mappings
            case "decimal" -> {}
            case "uuid" -> Types.UUIDType.get();
            case "date" -> {}
            case "time-millis", "time-micros" -> {}
            case "timestamp-millis", "timestamp-micros" -> {}
            case "local-timestamp-millis", "local-timestamp-micros" -> {}
            // standard avro types
            case "boolean" -> {}
            case "int" -> {}
            case "long" -> {}
            case "string", "enum" -> {}
            case "fixed" -> {}
            case "bytes" -> {}
            case "float" -> {}
            case "double" -> {}
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }

    private Object toIcebergValue(org.apache.avro.Schema avroValueSchema) {
        var valueType = avroValueSchema.getType().getName().toLowerCase();
        var connectLogicalType = avroValueSchema.getProp("connect.name");
        if (connectLogicalType != null) {
            valueType = connectLogicalType;
        }

        return switch (valueType) {
            // debezium logical type mappings
            case "io.debezium.data.Bits" -> {}
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> {}
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime" -> {}
            case "io.debezium.time.MicroDuration" -> {}
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Uuid", "io.debezium.data.Enum" -> {}
            case "io.debezium.time.Date" -> {}
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> {}
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp", "io.debezium.time.NanoTimestamp" -> {}
            case "io.debezium.time.IsoDate" -> {}
            case "io.debezium.data.VariableScaleDecimal" -> {}
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> {}
            case "org.apache.kafka.connect.data.Date" -> {}
            case "org.apache.kafka.connect.data.Time" -> {}
            case "org.apache.kafka.connect.data.Timestamp" -> {}
            // avro logical type mappings
            case "decimal" -> {}
            case "uuid" -> {}
            case "date" -> {}
            case "time-millis", "time-micros" -> {}
            case "timestamp-millis", "timestamp-micros" -> {}
            case "local-timestamp-millis", "local-timestamp-micros" -> {}
            // standard avro types
            case "boolean", "int", "long" -> {}
            case "string", "enum", "bytes", "fixed" -> {}
            case "float", "double" -> {}
            case "union" -> {}
            case "map" -> {}
            case "record" -> {}
            case "array" -> {}
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
    }
}
