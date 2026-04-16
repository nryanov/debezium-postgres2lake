package io.debezium.postgres2lake.iceberg.infrastructure;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.iceberg.infrastructure.profile.IcebergOutputFormatProfile;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.iceberg.test.helper.IcebergHelper;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.jdbc.JdbcCatalog;
import org.jboss.logging.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

@QuarkusTest
@TestProfile(IcebergOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot"),
        @ResourceArg(name = PostgresResource.CATALOG_TYPE_ARG, value = "iceberg")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "iceberg")
})
public class IcebergEventSaverTest {
    private final static Logger logger = Logger.getLogger(IcebergEventSaverTest.class);

    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";

    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    private static IcebergHelper icebergHelper;

    @BeforeEach
    public void setupIcebergHelper() {
        if (icebergHelper == null) {
            icebergHelper = IcebergHelper.jdbc(String.format("s3a://%s", BUCKET), postgresHelper, minioHelper);
        }
    }

    @AfterAll
    public static void cleanup() {
        if (icebergHelper != null) {
            ((JdbcCatalog) icebergHelper.getCatalog()).close();
        }
    }

    @Test
    void testSmallintType() {
        var table = "public.test_smallint";
        postgresHelper.executeSql(PostgresQueries.createSmallintTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertSmallintRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_smallint");

        assertEquals(row.getField("required_field"), 1);
        assertEquals(row.getField("optional_field"), 2);
        assertEquals(row.getField("required_array_field"), List.of(1, 2));
        assertEquals(row.getField("optional_array_field"), List.of(3, 4));
    }

    @Test
    void testIntegerType() {
        var table = "public.test_integer";
        postgresHelper.executeSql(PostgresQueries.createIntegerTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertIntegerRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_integer");

        assertEquals(row.getField("required_field"), 1);
        assertEquals(row.getField("optional_field"), 2);
        assertEquals(row.getField("required_array_field"), List.of(1, 2));
        assertEquals(row.getField("optional_array_field"), List.of(3, 4));
    }

    @Test
    void testBigintType() {
        var table = "public.test_bigint";
        postgresHelper.executeSql(PostgresQueries.createBigintTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertBigintRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_bigint");

        assertEquals(row.getField("required_field"), 1L);
        assertEquals(row.getField("optional_field"), 2L);
        assertEquals(row.getField("required_array_field"), List.of(1L, 2L));
        assertEquals(row.getField("optional_array_field"), List.of(3L, 4L));
    }

    @Test
    void testDecimalType() {
        var table = "public.test_decimal";
        postgresHelper.executeSql(PostgresQueries.createDecimalTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDecimalRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_decimal");

        assertEquals(row.getField("required_field"), 1.5);
        assertEquals(row.getField("optional_field"), 2.5);
        assertEquals(row.getField("required_array_field"), List.of(1.0, 2.0));
        assertEquals(row.getField("optional_array_field"), List.of(3.0, 4.0));
    }

    @Test
    void testNumericType() {
        var table = "public.test_numeric";
        postgresHelper.executeSql(PostgresQueries.createNumericTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertNumericRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_numeric");

        assertEquals(row.getField("required_field"), new BigDecimal("1.1234567890"));
        assertEquals(row.getField("optional_field"), new BigDecimal("2.1234567890"));
        assertEquals(row.getField("required_array_field"), List.of(new BigDecimal("1.1234567890"), new BigDecimal("2.1234567890")));
        assertEquals(row.getField("optional_array_field"), List.of(new BigDecimal("3.1234567890"), new BigDecimal("4.1234567890")));
    }

    @Test
    void testRealType() {
        var table = "public.test_real";
        postgresHelper.executeSql(PostgresQueries.createRealTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertRealRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_real");

        assertEquals(row.getField("required_field"), 1.5f);
        assertEquals(row.getField("optional_field"), 2.5f);
        assertEquals(row.getField("required_array_field"), List.of(1.0f, 2.0f));
        assertEquals(row.getField("optional_array_field"), List.of(3.0f, 4.0f));
    }

    @Test
    void testDoublePrecisionType() {
        var table = "public.test_double_precision";
        postgresHelper.executeSql(PostgresQueries.createDoublePrecisionTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDoublePrecisionRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_double_precision");

        assertEquals(row.getField("required_field"), 1.5);
        assertEquals(row.getField("optional_field"), 2.5);
        assertEquals(row.getField("required_array_field"), List.of(1.0, 2.0));
        assertEquals(row.getField("optional_array_field"), List.of(3.0, 4.0));
    }

    @Test
    void testCharType() {
        var table = "public.test_char";
        postgresHelper.executeSql(PostgresQueries.createCharTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertCharRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_char");

        assertEquals(row.getField("required_field"), "a");
        assertEquals(row.getField("optional_field"), "b");
        assertEquals(row.getField("required_array_field"), List.of("a", "b"));
        assertEquals(row.getField("optional_array_field"), List.of("c", "d"));
    }

    @Test
    void testVarcharType() {
        var table = "public.test_varchar";
        postgresHelper.executeSql(PostgresQueries.createVarcharTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertVarcharRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_varchar");

        assertEquals(row.getField("required_field"), "abc");
        assertEquals(row.getField("optional_field"), "def");
        assertEquals(row.getField("required_array_field"), List.of("abc", "def"));
        assertEquals(row.getField("optional_array_field"), List.of("ghi", "jkl"));
    }

    @Test
    void testTextType() {
        var table = "public.test_text";
        postgresHelper.executeSql(PostgresQueries.createTextTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTextRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_text");

        assertEquals(row.getField("required_field"), "hello");
        assertEquals(row.getField("optional_field"), "world");
        assertEquals(row.getField("required_array_field"), List.of("hello", "world"));
        assertEquals(row.getField("optional_array_field"), List.of("foo", "bar"));
    }

    @Test
    void testTimestampType() {
        var table = "public.test_timestamp";
        postgresHelper.executeSql(PostgresQueries.createTimestampTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimestampRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_timestamp");

        assertEquals(row.getField("required_field"), LocalDateTime.parse("2020-01-01T12:00:00"));
        assertEquals(row.getField("optional_field"), LocalDateTime.parse("2020-06-15T18:30:00"));
        assertEquals(row.getField("required_array_field"), List.of(
                LocalDateTime.parse("2020-01-01T12:00:00"),
                LocalDateTime.parse("2020-01-02T12:00:00")));
        assertEquals(row.getField("optional_array_field"), List.of(
                LocalDateTime.parse("2020-06-15T18:30:00"),
                LocalDateTime.parse("2020-06-16T18:30:00")));
    }

    @Test
    void testTimestampTzType() {
        var table = "public.test_timestamp_tz";
        postgresHelper.executeSql(PostgresQueries.createTimestampTzTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimestampTzRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_timestamp_tz");

        assertEquals(row.getField("required_field"), OffsetDateTime.parse("2020-01-01T12:00:00Z"));
        assertEquals(row.getField("optional_field"), OffsetDateTime.parse("2020-06-15T18:30:00Z"));
        assertEquals(row.getField("required_array_field"), List.of(
                OffsetDateTime.parse("2020-01-01T12:00:00Z"),
                OffsetDateTime.parse("2020-01-02T12:00:00Z")));
        assertEquals(row.getField("optional_array_field"), List.of(
                OffsetDateTime.parse("2020-06-15T18:30:00Z"),
                OffsetDateTime.parse("2020-06-16T18:30:00Z")));
    }

    @Test
    void testDateType() {
        var table = "public.test_date";
        postgresHelper.executeSql(PostgresQueries.createDateTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDateRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_date");

        assertEquals(row.getField("required_field"), LocalDate.of(2020, 1, 1));
        assertEquals(row.getField("optional_field"), LocalDate.of(2020, 6, 15));
        assertEquals(row.getField("required_array_field"), List.of(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(2020, 1, 2)));
        assertEquals(row.getField("optional_array_field"), List.of(
                LocalDate.of(2020, 6, 15),
                LocalDate.of(2020, 6, 16)));
    }

    @Test
    void testTimeWithoutTimeZoneType() {
        var table = "public.test_time";
        postgresHelper.executeSql(PostgresQueries.createTimeTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimeRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_time");

        assertEquals(row.getField("required_field"), LocalTime.of(12, 34, 56));
        assertEquals(row.getField("optional_field"), LocalTime.of(8, 15, 30));
        assertEquals(row.getField("required_array_field"), List.of(
                LocalTime.of(12, 0, 0),
                LocalTime.of(13, 0, 0)));
        assertEquals(row.getField("optional_array_field"), List.of(
                LocalTime.of(8, 0, 0),
                LocalTime.of(9, 0, 0)));
    }

    @Test
    void testBooleanType() {
        var table = "public.test_boolean";
        postgresHelper.executeSql(PostgresQueries.createBooleanTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertBooleanRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_boolean");

        assertEquals(row.getField("required_field"), true);
        assertEquals(row.getField("optional_field"), false);
        assertEquals(row.getField("required_array_field"), List.of(true, false));
        assertEquals(row.getField("optional_array_field"), List.of(false, true));
    }

    @Test
    void testUuidType() {
        var table = "public.test_uuid";
        postgresHelper.executeSql(PostgresQueries.createUuidTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertUuidRow(table, 1));

        var row = waitAndReadIcebergRecord("default_public", "test_uuid");

        assertEquals(row.getField("required_field"), UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals(row.getField("optional_field"), UUID.fromString("650e8400-e29b-41d4-a716-446655440000"));
        assertEquals(row.getField("required_array_field"), List.of(
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                UUID.fromString("650e8400-e29b-41d4-a716-446655440000")));
        assertEquals(row.getField("optional_array_field"), List.of(
                UUID.fromString("750e8400-e29b-41d4-a716-446655440000"),
                UUID.fromString("850e8400-e29b-41d4-a716-446655440000")));
    }

    private Record waitAndReadIcebergRecord(String schema, String table) {
        var saver = (AbstractEventSaver<?>) eventSaver;
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> saver.getCurrentRecords() > 0);
        eventSaver.flush();

        try (var data = icebergHelper.readTable(schema, table)) {
            var iterator = data.iterator();
            assertTrue(iterator.hasNext(), "No iceberg data found for table: " + schema + "." + table);
            var record = iterator.next();
            iterator.close();
            return record;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
