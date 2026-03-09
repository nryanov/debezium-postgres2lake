package io.debezium.postgres2lake.infrastructure.format.iceberg;

import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types;

import java.util.ArrayList;

public class AvroToIcebergMapper {
    public Schema avroToIcebergSchema(org.apache.avro.Schema keySchema, org.apache.avro.Schema avroValueSchema) {
        Types.NestedField.builder().build();
        Types.StructType.of();
        Types.ListType.ofOptional();
        Types.MapType.ofOptional();

        var fields = new ArrayList<Types.NestedField>();

        return new Schema(fields);
    }

    private Types.NestedField toIcebergSchema(org.apache.avro.Schema avroValueSchema) {
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
