package io.debezium.postgres2lake.iceberg.infrastructure;

import io.debezium.postgres2lake.iceberg.infrastructure.format.iceberg.IcebergSchemaConverter;
import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;

import static io.debezium.postgres2lake.test.avro.AvroTestFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

public class IcebergSchemaConverterTest {

    private final IcebergSchemaConverter converter = new IcebergSchemaConverter();

    private static void assertStringType(Type t) {
        assertEquals(Types.StringType.get(), t);
    }

    private org.apache.iceberg.Schema extractSingleValueField(Schema avroFieldSchema) {
        var keySchema = record("Key", List.of());
        var valueSchema = record("Val", List.of(field("v", avroFieldSchema)));
        return converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));
    }

    private Types.NestedField assertValueFieldType(Schema avroFieldSchema) {
        var schema = extractSingleValueField(avroFieldSchema);
        assertTrue(schema.identifierFieldIds().isEmpty());
        assertEquals(1, schema.columns().size());
        var f = schema.findField("v");
        assertNotNull(f);
        return f;
    }

    @Test
    void intType() {
        var f = assertValueFieldType(required(Schema.Type.INT));
        assertInstanceOf(Types.IntegerType.class, f.type());
    }

    @Test
    void longType() {
        var f = assertValueFieldType(required(Schema.Type.LONG));
        assertInstanceOf(Types.LongType.class, f.type());
    }

    @Test
    void floatType() {
        var f = assertValueFieldType(required(Schema.Type.FLOAT));
        assertInstanceOf(Types.FloatType.class, f.type());
    }

    @Test
    void doubleType() {
        var f = assertValueFieldType(required(Schema.Type.DOUBLE));
        assertInstanceOf(Types.DoubleType.class, f.type());
    }

    @Test
    void booleanType() {
        var f = assertValueFieldType(required(Schema.Type.BOOLEAN));
        assertInstanceOf(Types.BooleanType.class, f.type());
    }

    @Test
    void stringType() {
        var f = assertValueFieldType(required(Schema.Type.STRING));
        assertStringType(f.type());
    }

    @Test
    void enumType() {
        var enumSchema = Schema.createEnum("E", null, TEST_NAMESPACE, List.of("A", "B"));
        var f = assertValueFieldType(enumSchema);
        assertStringType(f.type());
    }

    @Test
    void bytesType() {
        var f = assertValueFieldType(required(Schema.Type.BYTES));
        assertEquals(Types.BinaryType.get(), f.type());
    }

    @Test
    void fixedType() {
        var fixed = Schema.createFixed("Fx", null, TEST_NAMESPACE, 16);
        var f = assertValueFieldType(fixed);
        var fixedType = assertInstanceOf(Types.FixedType.class, f.type());
        assertEquals(16, fixedType.length());
    }

    @Test
    void nullableUnionAddsNullability() {
        var f = assertValueFieldType(nullable(required(Schema.Type.INT)));
        assertTrue(f.isOptional());
        assertInstanceOf(Types.IntegerType.class, f.type());
    }

    @Test
    void arrayType() {
        var avro = Schema.createArray(required(Schema.Type.LONG));
        var f = assertValueFieldType(avro);
        var list = assertInstanceOf(Types.ListType.class, f.type());
        assertInstanceOf(Types.LongType.class, list.elementType());
    }

    @Test
    void mapType() {
        var avro = Schema.createMap(required(Schema.Type.INT));
        var f = assertValueFieldType(avro);
        var map = assertInstanceOf(Types.MapType.class, f.type());
        assertStringType(map.keyType());
        assertInstanceOf(Types.IntegerType.class, map.valueType());
    }

    @Test
    void nestedRecord() {
        var inner = record("Inner", List.of(field("x", required(Schema.Type.STRING))));
        var root = record("Root", List.of(field("id", required(Schema.Type.LONG)), field("nested", inner)));
        var keySchema = record("Key", List.of());
        var icebergSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(root)));

        assertTrue(icebergSchema.identifierFieldIds().isEmpty());
        assertEquals(2, icebergSchema.columns().size());
        assertEquals("id", icebergSchema.columns().get(0).name());
        assertInstanceOf(Types.LongType.class, icebergSchema.columns().get(0).type());
        assertEquals("nested", icebergSchema.columns().get(1).name());
        var nested = assertInstanceOf(Types.StructType.class, icebergSchema.columns().get(1).type());
        assertEquals(1, nested.fields().size());
        assertEquals("x", nested.fields().get(0).name());
        assertStringType(nested.fields().get(0).type());
    }

    @Test
    void logicalDecimal() {
        var f = assertValueFieldType(decimalSchema(10, 4));
        var dec = assertInstanceOf(Types.DecimalType.class, f.type());
        assertEquals(10, dec.precision());
        assertEquals(4, dec.scale());
    }

    @Test
    void logicalUuid() {
        var avro = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType());
        var f = assertValueFieldType(avro);
        assertEquals(Types.UUIDType.get(), f.type());
    }

    @Test
    void logicalDate() {
        var avro = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        var f = assertValueFieldType(avro);
        assertEquals(Types.DateType.get(), f.type());
    }

    @Test
    void logicalTimeMillis() {
        var avro = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        var f = assertValueFieldType(avro);
        assertEquals(Types.TimeType.get(), f.type());
    }

    @Test
    void logicalTimeMicros() {
        var avro = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        var f = assertValueFieldType(avro);
        assertEquals(Types.TimeType.get(), f.type());
    }

    @Test
    void logicalTimestampMillisAdjustToUtc() {
        var f = assertValueFieldType(timestampMillisLong(true));
        var ts = assertInstanceOf(Types.TimestampType.class, f.type());
        assertTrue(ts.shouldAdjustToUTC());
    }

    @Test
    void logicalTimestampMillisLocalZoned() {
        var f = assertValueFieldType(timestampMillisLong(false));
        var ts = assertInstanceOf(Types.TimestampType.class, f.type());
        assertFalse(ts.shouldAdjustToUTC());
    }

    @Test
    void logicalTimestampMicrosAdjustToUtc() {
        var f = assertValueFieldType(timestampMicrosLong(true));
        var ts = assertInstanceOf(Types.TimestampType.class, f.type());
        assertTrue(ts.shouldAdjustToUTC());
    }

    @Test
    void logicalTimestampMicrosLocalZoned() {
        var f = assertValueFieldType(timestampMicrosLong(false));
        var ts = assertInstanceOf(Types.TimestampType.class, f.type());
        assertFalse(ts.shouldAdjustToUTC());
    }

    @Test
    void logicalLocalTimestampMillis() {
        var avro = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType());
        var f = assertValueFieldType(avro);
        assertInstanceOf(Types.LongType.class, f.type());
    }

    @Test
    void logicalLocalTimestampMicros() {
        var avro = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
        var f = assertValueFieldType(avro);
        assertInstanceOf(Types.LongType.class, f.type());
    }

    @Test
    void extractSchemaPrimaryKeyAndColumns() {
        var keySchema = record("Key", List.of(field("col_a", required(Schema.Type.INT))));
        var valueSchema = record(
                "Val",
                List.of(field("col_a", required(Schema.Type.INT)), field("col_b", required(Schema.Type.BOOLEAN))));

        var icebergSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));

        var colA = icebergSchema.findField("col_a");
        assertNotNull(colA);
        assertEquals(Set.of(colA.fieldId()), icebergSchema.identifierFieldIds());

        var columns = icebergSchema.columns();
        assertEquals(2, columns.size());
        assertEquals("col_a", columns.get(0).name());
        assertInstanceOf(Types.IntegerType.class, columns.get(0).type());
        assertEquals("col_b", columns.get(1).name());
        assertInstanceOf(Types.BooleanType.class, columns.get(1).type());
    }

    @Test
    void extractSchemaCompositePrimaryKey() {
        var keySchema = record(
                "Key",
                List.of(field("a", required(Schema.Type.INT)), field("b", required(Schema.Type.LONG))));
        var valueSchema = record(
                "Val",
                List.of(
                        field("a", required(Schema.Type.INT)),
                        field("b", required(Schema.Type.LONG)),
                        field("c", required(Schema.Type.STRING))));

        var icebergSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));

        var idA = icebergSchema.findField("a").fieldId();
        var idB = icebergSchema.findField("b").fieldId();
        assertEquals(Set.of(idA, idB), icebergSchema.identifierFieldIds());
        assertEquals(3, icebergSchema.columns().size());
    }

    @Test
    void extractSchemaEmptyKey() {
        var keySchema = record("Key", List.of());
        var valueSchema = record("Val", List.of(field("x", required(Schema.Type.STRING))));

        var icebergSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));

        assertTrue(icebergSchema.identifierFieldIds().isEmpty());
        assertNotNull(icebergSchema.findField("x"));
    }

    @Test
    void extractSchemaKeyFieldMissingFromValue() {
        var keySchema = record("Key", List.of(field("only_in_key", required(Schema.Type.INT))));
        var valueSchema = record("Val", List.of(field("col_x", required(Schema.Type.STRING))));

        var icebergSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));

        assertTrue(icebergSchema.identifierFieldIds().isEmpty());
    }
}
