package io.debezium.postgres2lake.core.infrastructure.schema;

import io.debezium.postgres2lake.domain.model.AvroSchemaChanges;
import io.debezium.postgres2lake.domain.model.AvroSchemaChanges.ColumnChange;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.IncompatibleChangeToRequiredException;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.IncompatibleTypePromotion;
import io.debezium.postgres2lake.core.infrastructure.schema.exceptions.NonPrimitiveFieldPromotionException;
import org.apache.avro.Schema;
import org.junit.jupiter.api.Test;

import java.util.List;

import static io.debezium.postgres2lake.test.avro.AvroTestFixtures.*;
import static org.junit.jupiter.api.Assertions.*;

public class SchemaDiffResolverTest {

    private final SchemaDiffResolver resolver = new SchemaDiffResolver();

    @Test
    void addPrimitiveColumn() {
        var nextTag = nullable(required(Schema.Type.STRING));
        var current = record("Root", List.of(field("id", required(Schema.Type.LONG))));
        var next = record(
                "Root",
                List.of(field("id", required(Schema.Type.LONG)), field("tag", nextTag)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges =
                List.of(new AvroSchemaChanges.AddColumn(List.of(), "tag", nextTag));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void addComplexColumn() {
        var inner = record("Inner", List.of(field("a", required(Schema.Type.STRING))));
        var current = record("Root", List.of(field("id", required(Schema.Type.LONG))));
        var next = record(
                "Root",
                List.of(field("id", required(Schema.Type.LONG)), field("payload", inner)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(new AvroSchemaChanges.AddColumn(List.of(), "payload", inner));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void dropColumn() {
        var current = record(
                "Root",
                List.of(field("id", required(Schema.Type.LONG)), field("dropMe", required(Schema.Type.STRING))));
        var next = record("Root", List.of(field("id", required(Schema.Type.LONG))));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(new AvroSchemaChanges.DeleteColumn(List.of(), "dropMe"));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void dropNestedColumn() {
        var nestedBefore = record(
                "Nested",
                List.of(field("keep", required(Schema.Type.INT)), field("gone", required(Schema.Type.STRING))));
        var nestedAfter = record("Nested", List.of(field("keep", required(Schema.Type.INT))));
        var current = record("Root", List.of(field("nested", nestedBefore)));
        var next = record("Root", List.of(field("nested", nestedAfter)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(new AvroSchemaChanges.DeleteColumn(List.of("nested"), "gone"));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void alterColumnSupportedIntToLong() {
        var current = record("Root", List.of(field("n", required(Schema.Type.INT))));
        var next = record("Root", List.of(field("n", required(Schema.Type.LONG))));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges =
                List.of(new AvroSchemaChanges.WideColumnType(List.of(), "n", required(Schema.Type.LONG)));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void makeColumnOptional() {
        var current = record("Root", List.of(field("n", required(Schema.Type.INT))));
        var next = record("Root", List.of(field("n", nullable(required(Schema.Type.INT)))));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(new AvroSchemaChanges.MakeOptional(List.of(), "n"));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void incorrectAlterOptionalToRequired() {
        var current = record("Root", List.of(field("n", nullable(required(Schema.Type.INT)))));
        var next = record("Root", List.of(field("n", required(Schema.Type.INT))));

        assertThrows(
                IncompatibleChangeToRequiredException.class, () -> resolver.resolveDiff(current, next));
    }

    @Test
    void incorrectAlterPrimitiveToComplex() {
        var inner = record("Inner", List.of(field("a", required(Schema.Type.INT))));
        var current = record("Root", List.of(field("x", required(Schema.Type.STRING))));
        var next = record("Root", List.of(field("x", inner)));

        assertThrows(NonPrimitiveFieldPromotionException.class, () -> resolver.resolveDiff(current, next));
    }

    @Test
    void incorrectAlterUnsupportedPromotion_longToInt() {
        var current = record("Root", List.of(field("n", required(Schema.Type.LONG))));
        var next = record("Root", List.of(field("n", required(Schema.Type.INT))));

        assertThrows(IncompatibleTypePromotion.class, () -> resolver.resolveDiff(current, next));
    }

    @Test
    void incorrectAlterUnsupportedPromotion_stringToInt() {
        var current = record("Root", List.of(field("n", required(Schema.Type.STRING))));
        var next = record("Root", List.of(field("n", required(Schema.Type.INT))));

        assertThrows(IncompatibleTypePromotion.class, () -> resolver.resolveDiff(current, next));
    }

    @Test
    void combinedMakeOptionalAndWidenRequiredIntToOptionalLong() {
        var current = record("Root", List.of(field("n", required(Schema.Type.INT))));
        var nextSchema = nullable(required(Schema.Type.LONG));
        var next = record("Root", List.of(field("n", nextSchema)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(
                new AvroSchemaChanges.MakeOptional(List.of(), "n"),
                new AvroSchemaChanges.WideColumnType(List.of(), "n", nextSchema));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void incorrectAlterComplexToPrimitive() {
        var inner = record("Inner", List.of(field("a", required(Schema.Type.INT))));
        var current = record("Root", List.of(field("x", inner)));
        var next = record("Root", List.of(field("x", required(Schema.Type.STRING))));

        assertThrows(NonPrimitiveFieldPromotionException.class, () -> resolver.resolveDiff(current, next));
    }

    @Test
    void alterColumnDecimalPrecisionWiden() {
        var curDec = decimalSchema(10, 2);
        var nextDec = decimalSchema(18, 2);
        var current = record("Root", List.of(field("amt", curDec)));
        var next = record("Root", List.of(field("amt", nextDec)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(new AvroSchemaChanges.WideColumnType(List.of(), "amt", nextDec));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void alterColumnFloatToDouble() {
        var current = record("Root", List.of(field("x", required(Schema.Type.FLOAT))));
        var next = record("Root", List.of(field("x", required(Schema.Type.DOUBLE))));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges =
                List.of(new AvroSchemaChanges.WideColumnType(List.of(), "x", required(Schema.Type.DOUBLE)));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void identicalSchemasYieldsNoChanges() {
        var current = record(
                "Root",
                List.of(field("id", required(Schema.Type.LONG)), field("tag", nullable(required(Schema.Type.STRING)))));
        var next = record(
                "Root",
                List.of(field("id", required(Schema.Type.LONG)), field("tag", nullable(required(Schema.Type.STRING)))));

        var changes = resolver.resolveDiff(current, next).changes();

        assertEquals(List.<ColumnChange>of(), changes);
    }

    @Test
    void nestedSupportedIntToLong() {
        var nestedBefore = record("Nested", List.of(field("k", required(Schema.Type.INT))));
        var nestedAfter = record("Nested", List.of(field("k", required(Schema.Type.LONG))));
        var current = record("Root", List.of(field("nested", nestedBefore)));
        var next = record("Root", List.of(field("nested", nestedAfter)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges =
                List.of(new AvroSchemaChanges.WideColumnType(List.of("nested"), "k", required(Schema.Type.LONG)));

        assertEquals(expectedChanges, changes);
    }

    @Test
    void multiFieldDiffChangeTypeMultiset() {
        var nestedBefore = record("Nested", List.of(field("k", required(Schema.Type.INT))));
        var nestedAfter = record("Nested", List.of(field("k", required(Schema.Type.LONG))));
        var addedSchema = nullable(required(Schema.Type.BOOLEAN));
        var current = record(
                "Root",
                List.of(
                        field("id", required(Schema.Type.LONG)),
                        field("remove", required(Schema.Type.STRING)),
                        field("nested", nestedBefore)));
        var next = record(
                "Root",
                List.of(
                        field("id", required(Schema.Type.LONG)),
                        field("nested", nestedAfter),
                        field("added", addedSchema)));

        var changes = resolver.resolveDiff(current, next).changes();
        List<ColumnChange> expectedChanges = List.of(
                new AvroSchemaChanges.DeleteColumn(List.of(), "remove"),
                new AvroSchemaChanges.WideColumnType(List.of("nested"), "k", required(Schema.Type.LONG)),
                new AvroSchemaChanges.AddColumn(List.of(), "added", addedSchema));

        assertEquals(expectedChanges, changes);
    }
}
