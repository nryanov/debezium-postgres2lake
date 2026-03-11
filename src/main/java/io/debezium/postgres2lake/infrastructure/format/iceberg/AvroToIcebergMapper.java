package io.debezium.postgres2lake.infrastructure.format.iceberg;

import org.apache.avro.LogicalTypes;
import org.apache.iceberg.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static io.debezium.postgres2lake.infrastructure.format.avro.AvroUtils.*;

public class AvroToIcebergMapper {
    public Schema avroToIcebergSchema(org.apache.avro.Schema keySchema, org.apache.avro.Schema avroValueSchema) {
        var fieldId = new AtomicInteger(0);
        // todo: add information about PK
        return toIcebergSchema(avroValueSchema, fieldId).asStructType().asSchema();
    }

    private Type toIcebergSchema(org.apache.avro.Schema schema, AtomicInteger fieldId) {
        return switch (schema.getType()) {
            case RECORD -> {
                var nestedFields = new ArrayList<Types.NestedField>();
                for (var field : schema.getFields()) {
                    var type = toIcebergSchema(field.schema(), fieldId);
                    var isOptional = isOptional(field.schema());
                    var id = fieldId.incrementAndGet();
                    var nestedField = Types.NestedField.builder().isOptional(isOptional).withDoc(field.doc()).withName(field.name()).withId(id).ofType(type).build();
                    nestedFields.add(nestedField);
                }

                yield Types.StructType.of(nestedFields);
            }
            case UNION -> {
                if (schema.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL) {
                    yield toIcebergSchema(schema.getTypes().get(1), fieldId);
                } else yield toIcebergSchema(schema.getTypes().get(0), fieldId);
            }
            case ARRAY -> {
                var elementSchema = schema.getElementType();
                var id = fieldId.incrementAndGet();
                if (AvroSchemaUtil.isOptionSchema(elementSchema)) {
                    yield Types.ListType.ofOptional(id, toIcebergSchema(elementSchema, fieldId));
                } else {
                    yield Types.ListType.ofRequired(id, toIcebergSchema(elementSchema, fieldId));
                }
            }
            case MAP -> {
                var valueSchema = schema.getValueType();
                int keyId = fieldId.incrementAndGet();
                int valueId = fieldId.incrementAndGet();

                if (AvroSchemaUtil.isOptional(valueSchema)) {
                    yield Types.MapType.ofOptional(keyId, valueId, Types.StringType.get(), toIcebergSchema(valueSchema, fieldId));
                } else {
                    yield Types.MapType.ofRequired(keyId, valueId, Types.StringType.get(), toIcebergSchema(valueSchema, fieldId));
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
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> Types.TimestampType.withZone();
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime" -> Types.TimeType.get();
            case "io.debezium.time.MicroDuration" -> Types.LongType.get();
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Enum" -> Types.StringType.get();
            case "io.debezium.data.Uuid" -> Types.UUIDType.get();
            case "io.debezium.time.Date" -> Types.DateType.get();
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> Types.TimeType.get();
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp" -> Types.TimestampType.withoutZone();
            case "io.debezium.time.NanoTimestamp" -> Types.TimestampNanoType.withoutZone();
            case "io.debezium.time.IsoDate" -> Types.DateType.get();
            case "io.debezium.data.VariableScaleDecimal" -> Types.DoubleType.get(); // todo: is it correct type?
            // kaka-connect logical type mappings
            case "org.apache.kafka.connect.data.Decimal" -> {
                var decimal = (LogicalTypes.Decimal) schema.getLogicalType();
                yield Types.DecimalType.of(decimal.getPrecision(), decimal.getScale());
            }
            case "org.apache.kafka.connect.data.Date" -> Types.DateType.get();
            case "org.apache.kafka.connect.data.Time" -> Types.TimeType.get();
            case "org.apache.kafka.connect.data.Timestamp" -> Types.TimestampType.withoutZone();
            // avro logical type mappings
            case "decimal" -> {
                var decimal = (LogicalTypes.Decimal) schema.getLogicalType();
                yield Types.DecimalType.of(decimal.getPrecision(), decimal.getScale());
            }
            case "uuid" -> Types.UUIDType.get();
            case "date" -> Types.DateType.get();
            case "time-millis", "time-micros" -> Types.TimeType.get();
            case "timestamp-millis", "timestamp-micros" -> Types.TimestampType.withZone();
            case "local-timestamp-millis", "local-timestamp-micros" -> Types.TimestampType.withoutZone();
            // standard avro types
            case "boolean" -> Types.BooleanType.get();
            case "int" -> Types.IntegerType.get();
            case "long" -> Types.LongType.get();
            case "string", "enum" -> Types.StringType.get();
            case "fixed" -> Types.FixedType.ofLength(schema.getFixedSize());
            case "bytes" -> Types.BinaryType.get();
            case "float" -> Types.FloatType.get();
            case "double" -> Types.DoubleType.get();
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };
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

        var valueType = avroSchema.getType().getName().toLowerCase();
        var connectLogicalType = avroSchema.getProp("connect.name");
        if (connectLogicalType != null) {
            valueType = connectLogicalType;
        }

        return switch (valueType) {
            // debezium logical type mappings
            case "io.debezium.data.Bits" -> convertToBytes(avroValue);
            case "io.debezium.time.ZonedTimestamp", "io.debezium.time.IsoTimestamp" -> {}
            case "io.debezium.time.ZonedTime", "io.debezium.time.IsoTime" -> {}
            case "io.debezium.time.MicroDuration" -> {}
            case "io.debezium.data.Json", "io.debezium.data.Xml", "io.debezium.data.Enum" -> convertToString(avroValue);
            case "io.debezium.data.Uuid" -> {}
            case "io.debezium.time.Date" -> {}
            case "io.debezium.time.Time", "io.debezium.time.MicroTime", "io.debezium.time.NanoTime" -> {}
            case "io.debezium.time.Timestamp", "io.debezium.time.MicroTimestamp" -> {}
            case "io.debezium.time.NanoTimestamp" -> {}
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
            case "boolean", "int", "long", "float", "double" -> avroValue;
            case "string", "enum" -> convertToString(avroValue);
            case "bytes" -> convertToBytes(avroValue);
            case "fixed" -> {}
            case "union" -> saveUnion(icebergField, avroSchema, avroValue);
            case "map" -> saveMap(icebergField, avroSchema, avroValue);
            case "record" -> saveInnerRecord(icebergField, avroSchema, avroValue);
            case "array" -> saveArray(icebergField, avroSchema, avroValue);
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

        rawArray.forEach(it -> {
            mappedArray.add(toIcebergValue(icebergField, avroSchema.getElementType(), it));
        });

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
