package io.debezium.postgres2lake.orc.infrastructure;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.orc.TypeDescription;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.debezium.postgres2lake.test.avro.AvroTestFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

public class OrcSchemaConverterTest {

    private final OrcSchemaConverter converter = new OrcSchemaConverter();

    private static TypeDescription unwrapValue(TypeDescription root) {
        assertEquals(TypeDescription.Category.STRUCT, root.getCategory());
        assertEquals(1, root.getChildren().size());
        assertEquals("v", root.getFieldNames().get(0));
        return root.getChildren().get(0);
    }

    @Test
    void intType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.INT))));
        assertEquals(TypeDescription.Category.INT, child.getCategory());
    }

    @Test
    void longType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.LONG))));
        assertEquals(TypeDescription.Category.LONG, child.getCategory());
    }

    @Test
    void floatType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.FLOAT))));
        assertEquals(TypeDescription.Category.FLOAT, child.getCategory());
    }

    @Test
    void doubleType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.DOUBLE))));
        assertEquals(TypeDescription.Category.DOUBLE, child.getCategory());
    }

    @Test
    void booleanType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.BOOLEAN))));
        assertEquals(TypeDescription.Category.BOOLEAN, child.getCategory());
    }

    @Test
    void stringType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.STRING))));
        assertEquals(TypeDescription.Category.STRING, child.getCategory());
    }

    @Test
    void enumType() {
        var enumSchema = Schema.createEnum("E", null, TEST_NAMESPACE, List.of("A", "B"));
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(enumSchema)));
        assertEquals(TypeDescription.Category.STRING, child.getCategory());
    }

    @Test
    void bytesType() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(required(Schema.Type.BYTES))));
        assertEquals(TypeDescription.Category.BINARY, child.getCategory());
    }

    @Test
    void fixedType() {
        var fixed = Schema.createFixed("Fx", null, TEST_NAMESPACE, 16);
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(fixed)));
        assertEquals(TypeDescription.Category.BINARY, child.getCategory());
    }

    @Test
    void nullableUnionUnwrapsNonNull() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(nullable(required(Schema.Type.INT)))));
        assertEquals(TypeDescription.Category.INT, child.getCategory());
    }

    @Test
    void arrayType() {
        var avro = Schema.createArray(required(Schema.Type.LONG));
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.LIST, child.getCategory());
        assertEquals(TypeDescription.Category.LONG, child.getChildren().get(0).getCategory());
    }

    @Test
    void mapType() {
        var avro = Schema.createMap(required(Schema.Type.INT));
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.MAP, child.getCategory());
        assertEquals(TypeDescription.Category.STRING, child.getChildren().get(0).getCategory());
        assertEquals(TypeDescription.Category.INT, child.getChildren().get(1).getCategory());
    }

    @Test
    void nestedRecord() {
        var inner = record("Inner", List.of(field("x", required(Schema.Type.STRING))));
        var root = record("Root", List.of(field("id", required(Schema.Type.LONG)), field("nested", inner)));
        var keySchema = record("Key", List.of());
        var td = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(root)));
        assertEquals(TypeDescription.Category.STRUCT, td.getCategory());
        assertEquals(List.of("id", "nested"), td.getFieldNames());
        assertEquals(TypeDescription.Category.LONG, td.getChildren().get(0).getCategory());
        var nested = td.getChildren().get(1);
        assertEquals(TypeDescription.Category.STRUCT, nested.getCategory());
        assertEquals("x", nested.getFieldNames().get(0));
        assertEquals(TypeDescription.Category.STRING, nested.getChildren().get(0).getCategory());
    }

    @Test
    void logicalDecimal() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(decimalSchema(10, 4))));
        assertEquals(TypeDescription.Category.DECIMAL, child.getCategory());
        assertEquals(10, child.getPrecision());
        assertEquals(4, child.getScale());
    }

    @Test
    void logicalUuid() {
        var avro = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.STRING, child.getCategory());
    }

    @Test
    void logicalDate() {
        var avro = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.DATE, child.getCategory());
    }

    @Test
    void logicalTimeMillis() {
        var avro = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.INT, child.getCategory());
    }

    @Test
    void logicalTimeMicros() {
        var avro = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.LONG, child.getCategory());
    }

    @Test
    void logicalTimestampMillisAdjustToUtc() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(timestampMillisLong(true))));
        assertEquals(TypeDescription.Category.TIMESTAMP, child.getCategory());
    }

    @Test
    void logicalTimestampMillisInstant() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(timestampMillisLong(false))));
        assertEquals(TypeDescription.Category.TIMESTAMP_INSTANT, child.getCategory());
    }

    @Test
    void logicalTimestampMicrosAdjustToUtc() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(timestampMicrosLong(true))));
        assertEquals(TypeDescription.Category.TIMESTAMP, child.getCategory());
    }

    @Test
    void logicalTimestampMicrosInstant() {
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(timestampMicrosLong(false))));
        assertEquals(TypeDescription.Category.TIMESTAMP_INSTANT, child.getCategory());
    }

    @Test
    void logicalLocalTimestampMillis() {
        var avro = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.TIMESTAMP_INSTANT, child.getCategory());
    }

    @Test
    void logicalLocalTimestampMicros() {
        var avro = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
        var child = unwrapValue(converter.extractSchema(singleFieldValueEvent(avro)));
        assertEquals(TypeDescription.Category.TIMESTAMP_INSTANT, child.getCategory());
    }
}
