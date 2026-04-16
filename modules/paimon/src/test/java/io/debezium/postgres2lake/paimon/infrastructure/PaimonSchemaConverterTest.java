package io.debezium.postgres2lake.paimon.infrastructure;

import org.apache.avro.LogicalTypes;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.paimon.types.ArrayType;
import org.apache.paimon.types.BinaryType;
import org.apache.paimon.types.BooleanType;
import org.apache.paimon.types.DataType;
import org.apache.paimon.types.DateType;
import org.apache.paimon.types.DecimalType;
import org.apache.paimon.types.DoubleType;
import org.apache.paimon.types.FloatType;
import org.apache.paimon.types.IntType;
import org.apache.paimon.types.LocalZonedTimestampType;
import org.apache.paimon.types.MapType;
import org.apache.paimon.types.BigIntType;
import org.apache.paimon.types.RowType;
import org.apache.paimon.types.TimeType;
import org.apache.paimon.types.TimestampType;
import org.apache.paimon.types.VarBinaryType;
import org.apache.paimon.types.VarCharType;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.debezium.postgres2lake.test.avro.AvroTestFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

public class PaimonSchemaConverterTest {

    private final PaimonSchemaConverter converter = new PaimonSchemaConverter();

    private static void assertStringType(DataType t) {
        assertInstanceOf(VarCharType.class, t);
    }

    @Test
    void intType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.INT));
        assertInstanceOf(IntType.class, dt);
    }

    @Test
    void longType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.LONG));
        assertInstanceOf(BigIntType.class, dt);
    }

    @Test
    void floatType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.FLOAT));
        assertInstanceOf(FloatType.class, dt);
    }

    @Test
    void doubleType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.DOUBLE));
        assertInstanceOf(DoubleType.class, dt);
    }

    @Test
    void booleanType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.BOOLEAN));
        assertInstanceOf(BooleanType.class, dt);
    }

    @Test
    void stringType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.STRING));
        assertStringType(dt);
    }

    @Test
    void enumType() {
        var enumSchema = Schema.createEnum("E", null, TEST_NAMESPACE, List.of("A", "B"));
        var dt = converter.convertAvroSchema(enumSchema);
        assertStringType(dt);
    }

    @Test
    void bytesType() {
        var dt = converter.convertAvroSchema(required(Schema.Type.BYTES));
        assertInstanceOf(VarBinaryType.class, dt);
    }

    @Test
    void fixedType() {
        var fixed = Schema.createFixed("Fx", null, TEST_NAMESPACE, 16);
        var dt = converter.convertAvroSchema(fixed);
        var bin = assertInstanceOf(BinaryType.class, dt);
        assertEquals(16, bin.getLength());
    }

    @Test
    void nullableUnionAddsNullability() {
        var dt = converter.convertAvroSchema(nullable(required(Schema.Type.INT)));
        assertInstanceOf(IntType.class, dt);
        assertTrue(dt.isNullable());
    }

    @Test
    void arrayType() {
        var avro = Schema.createArray(required(Schema.Type.LONG));
        var dt = converter.convertAvroSchema(avro);
        var arr = assertInstanceOf(ArrayType.class, dt);
        assertInstanceOf(BigIntType.class, arr.getElementType());
    }

    @Test
    void mapType() {
        var avro = Schema.createMap(required(Schema.Type.INT));
        var dt = converter.convertAvroSchema(avro);
        var map = assertInstanceOf(MapType.class, dt);
        assertStringType(map.getKeyType());
        assertInstanceOf(IntType.class, map.getValueType());
    }

    @Test
    void nestedRecord() {
        var inner = record("Inner", List.of(field("x", required(Schema.Type.STRING))));
        var root = record("Root", List.of(field("id", required(Schema.Type.LONG)), field("nested", inner)));
        var dt = converter.convertAvroSchema(root);
        var row = assertInstanceOf(RowType.class, dt);
        assertEquals(2, row.getFields().size());
        assertEquals("id", row.getFields().get(0).name());
        assertInstanceOf(BigIntType.class, row.getFields().get(0).type());
        assertEquals("nested", row.getFields().get(1).name());
        var nestedRow = assertInstanceOf(RowType.class, row.getFields().get(1).type());
        assertEquals(1, nestedRow.getFields().size());
        assertEquals("x", nestedRow.getFields().get(0).name());
        assertStringType(nestedRow.getFields().get(0).type());
    }

    @Test
    void logicalDecimal() {
        var dt = converter.convertAvroSchema(decimalSchema(10, 4));
        var dec = assertInstanceOf(DecimalType.class, dt);
        assertEquals(10, dec.getPrecision());
        assertEquals(4, dec.getScale());
    }

    @Test
    void logicalUuid() {
        var avro = LogicalTypes.uuid().addToSchema(SchemaBuilder.builder().stringType());
        var dt = converter.convertAvroSchema(avro);
        assertStringType(dt);
    }

    @Test
    void logicalDate() {
        var avro = LogicalTypes.date().addToSchema(SchemaBuilder.builder().intType());
        var dt = converter.convertAvroSchema(avro);
        assertInstanceOf(DateType.class, dt);
    }

    @Test
    void logicalTimeMillis() {
        var avro = LogicalTypes.timeMillis().addToSchema(SchemaBuilder.builder().intType());
        var dt = converter.convertAvroSchema(avro);
        var time = assertInstanceOf(TimeType.class, dt);
        assertEquals(3, time.getPrecision());
    }

    @Test
    void logicalTimeMicros() {
        var avro = LogicalTypes.timeMicros().addToSchema(SchemaBuilder.builder().longType());
        var dt = converter.convertAvroSchema(avro);
        var time = assertInstanceOf(TimeType.class, dt);
        assertEquals(3, time.getPrecision());
    }

    @Test
    void logicalTimestampMillisAdjustToUtc() {
        var dt = converter.convertAvroSchema(timestampMillisLong(true));
        var ts = assertInstanceOf(TimestampType.class, dt);
        assertEquals(3, ts.getPrecision());
    }

    @Test
    void logicalTimestampMillisLocalZoned() {
        var dt = converter.convertAvroSchema(timestampMillisLong(false));
        var ts = assertInstanceOf(LocalZonedTimestampType.class, dt);
        assertEquals(3, ts.getPrecision());
    }

    @Test
    void logicalTimestampMicrosAdjustToUtc() {
        var dt = converter.convertAvroSchema(timestampMicrosLong(true));
        var ts = assertInstanceOf(TimestampType.class, dt);
        assertEquals(6, ts.getPrecision());
    }

    @Test
    void logicalTimestampMicrosLocalZoned() {
        var dt = converter.convertAvroSchema(timestampMicrosLong(false));
        var ts = assertInstanceOf(LocalZonedTimestampType.class, dt);
        assertEquals(6, ts.getPrecision());
    }

    @Test
    void logicalLocalTimestampMillis() {
        var avro = LogicalTypes.localTimestampMillis().addToSchema(SchemaBuilder.builder().longType());
        var dt = converter.convertAvroSchema(avro);
        var ts = assertInstanceOf(LocalZonedTimestampType.class, dt);
        assertEquals(3, ts.getPrecision());
    }

    @Test
    void logicalLocalTimestampMicros() {
        var avro = LogicalTypes.localTimestampMicros().addToSchema(SchemaBuilder.builder().longType());
        var dt = converter.convertAvroSchema(avro);
        var ts = assertInstanceOf(LocalZonedTimestampType.class, dt);
        assertEquals(6, ts.getPrecision());
    }

    @Test
    void extractSchemaPrimaryKeyAndColumns() {
        var keySchema = record("Key", List.of(field("col_a", required(Schema.Type.INT))));
        var valueSchema = record(
                "Val",
                List.of(field("col_a", required(Schema.Type.INT)), field("col_b", required(Schema.Type.BOOLEAN))));

        var paimonSchema = converter.extractSchema(insertEvent(emptyRecord(keySchema), emptyRecord(valueSchema)));

        assertEquals(List.of("col_a"), paimonSchema.primaryKeys());

        var columns = paimonSchema.fields();
        assertEquals(2, columns.size());
        assertEquals("col_a", columns.get(0).name());
        assertInstanceOf(IntType.class, columns.get(0).type());
        assertEquals("col_b", columns.get(1).name());
        assertInstanceOf(BooleanType.class, columns.get(1).type());
    }
}
