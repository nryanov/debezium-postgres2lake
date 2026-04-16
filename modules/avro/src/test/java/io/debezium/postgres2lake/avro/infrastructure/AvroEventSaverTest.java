package io.debezium.postgres2lake.avro.infrastructure;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.avro.infrastructure.profile.AvroOutputFormatProfile;
import io.debezium.postgres2lake.core.service.AbstractEventSaver;
import io.debezium.postgres2lake.test.annotation.InjectMinioHelper;
import io.debezium.postgres2lake.test.annotation.InjectPostgresHelper;
import io.debezium.postgres2lake.test.helper.MinioHelper;
import io.debezium.postgres2lake.test.helper.PostgresHelper;
import io.debezium.postgres2lake.test.helper.PostgresQueries;
import io.debezium.postgres2lake.test.resource.MinioResource;
import io.debezium.postgres2lake.test.resource.PostgresResource;
import io.quarkus.test.common.QuarkusTestResource;
import io.quarkus.test.common.ResourceArg;
import io.quarkus.test.junit.QuarkusTest;
import io.quarkus.test.junit.TestProfile;
import jakarta.inject.Inject;
import org.apache.avro.Conversions;
import org.apache.avro.data.TimeConversions;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.SeekableByteArrayInput;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.math.BigDecimal;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.UUID;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

@QuarkusTest
@TestProfile(AvroOutputFormatProfile.class)
@QuarkusTestResource(value = PostgresResource.class, initArgs = {
        @ResourceArg(name = PostgresResource.PREFIX_NAME_ARG, value = "default"),
        @ResourceArg(name = PostgresResource.PUBLICATION_NAME_ARG, value = "debezium_publication"),
        @ResourceArg(name = PostgresResource.SLOT_NAME_ARG, value = "debezium_slot")
})
@QuarkusTestResource(value = MinioResource.class, initArgs = {
        @ResourceArg(name = MinioResource.BUCKET_NAME_ARG, value = "warehouse"),
        @ResourceArg(name = MinioResource.FORMAT_TYPE_ARG, value = "avro")
})
public class AvroEventSaverTest {
    private static final String BUCKET = "warehouse";
    private static final String PUBLICATION = "debezium_publication";

    static {
        var data = GenericData.get();
        data.addLogicalTypeConversion(new Conversions.DecimalConversion());
        data.addLogicalTypeConversion(new Conversions.UUIDConversion());
        data.addLogicalTypeConversion(new TimeConversions.DateConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimeMicrosConversion());
        data.addLogicalTypeConversion(new TimeConversions.TimestampMicrosConversion());
    }

    @Inject
    private EventSaver eventSaver;

    @InjectMinioHelper
    MinioHelper minioHelper;

    @InjectPostgresHelper
    PostgresHelper postgresHelper;

    @BeforeEach
    public void cleanup() {
        eventSaver.flush();
        minioHelper.clearBucket(BUCKET);
    }

    @Test
    void testSmallintType() {
        var table = "public.test_smallint";
        postgresHelper.executeSql(PostgresQueries.createSmallintTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertSmallintRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_smallint/");

        assertEquals(row.get("required_field"), 1);
        assertEquals(row.get("optional_field"), 2);
        assertEquals(row.get("required_array_field"), List.of(1, 2));
        assertEquals(row.get("optional_array_field"), List.of(3, 4));
    }

    @Test
    void testIntegerType() {
        var table = "public.test_integer";
        postgresHelper.executeSql(PostgresQueries.createIntegerTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertIntegerRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_integer/");

        assertEquals(row.get("required_field"), 1);
        assertEquals(row.get("optional_field"), 2);
        assertEquals(row.get("required_array_field"), List.of(1, 2));
        assertEquals(row.get("optional_array_field"), List.of(3, 4));
    }

    @Test
    void testBigintType() {
        var table = "public.test_bigint";
        postgresHelper.executeSql(PostgresQueries.createBigintTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertBigintRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_bigint/");

        assertEquals(row.get("required_field"), 1L);
        assertEquals(row.get("optional_field"), 2L);
        assertEquals(row.get("required_array_field"), List.of(1L, 2L));
        assertEquals(row.get("optional_array_field"), List.of(3L, 4L));
    }

    @Test
    void testDecimalType() {
        var table = "public.test_decimal";
        postgresHelper.executeSql(PostgresQueries.createDecimalTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDecimalRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_decimal/");

        assertEquals(row.get("required_field"), 1.5);
        assertEquals(row.get("optional_field"), 2.5);
        assertEquals(row.get("required_array_field"), List.of(1.0, 2.0));
        assertEquals(row.get("optional_array_field"), List.of(3.0, 4.0));
    }

    @Test
    void testNumericType() {
        var table = "public.test_numeric";
        postgresHelper.executeSql(PostgresQueries.createNumericTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertNumericRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_numeric/");

        assertEquals(row.get("required_field"), new BigDecimal("1.1234567890"));
        assertEquals(row.get("optional_field"), new BigDecimal("2.1234567890"));
        assertEquals(row.get("required_array_field"), List.of(new BigDecimal("1.1234567890"), new BigDecimal("2.1234567890")));
        assertEquals(row.get("optional_array_field"), List.of(new BigDecimal("3.1234567890"), new BigDecimal("4.1234567890")));
    }

    @Test
    void testRealType() {
        var table = "public.test_real";
        postgresHelper.executeSql(PostgresQueries.createRealTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertRealRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_real/");

        assertEquals(row.get("required_field"), 1.5f);
        assertEquals(row.get("optional_field"), 2.5f);
        assertEquals(row.get("required_array_field"), List.of(1.0f, 2.0f));
        assertEquals(row.get("optional_array_field"), List.of(3.0f, 4.0f));
    }

    @Test
    void testDoublePrecisionType() {
        var table = "public.test_double_precision";
        postgresHelper.executeSql(PostgresQueries.createDoublePrecisionTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDoublePrecisionRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_double_precision/");

        assertEquals(row.get("required_field"), 1.5);
        assertEquals(row.get("optional_field"), 2.5);
        assertEquals(row.get("required_array_field"), List.of(1.0, 2.0));
        assertEquals(row.get("optional_array_field"), List.of(3.0, 4.0));
    }

    @Test
    void testCharType() {
        var table = "public.test_char";
        postgresHelper.executeSql(PostgresQueries.createCharTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertCharRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_char/");

        assertEquals(row.get("required_field"), new Utf8("a"));
        assertEquals(row.get("optional_field"), new Utf8("b"));
        assertEquals(row.get("required_array_field"), List.of(new Utf8("a"), new Utf8("b")));
        assertEquals(row.get("optional_array_field"), List.of(new Utf8("c"), new Utf8("d")));
    }

    @Test
    void testVarcharType() {
        var table = "public.test_varchar";
        postgresHelper.executeSql(PostgresQueries.createVarcharTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertVarcharRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_varchar/");

        assertEquals(row.get("required_field"), new Utf8("abc"));
        assertEquals(row.get("optional_field"), new Utf8("def"));
        assertEquals(row.get("required_array_field"), List.of(new Utf8("abc"), new Utf8("def")));
        assertEquals(row.get("optional_array_field"), List.of(new Utf8("ghi"), new Utf8("jkl")));
    }

    @Test
    void testTextType() {
        var table = "public.test_text";
        postgresHelper.executeSql(PostgresQueries.createTextTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTextRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_text/");

        assertEquals(row.get("required_field"), new Utf8("hello"));
        assertEquals(row.get("optional_field"), new Utf8("world"));
        assertEquals(row.get("required_array_field"), List.of(new Utf8("hello"), new Utf8("world")));
        assertEquals(row.get("optional_array_field"), List.of(new Utf8("foo"), new Utf8("bar")));
    }

    @Test
    void testTimestampType() {
        var table = "public.test_timestamp";
        postgresHelper.executeSql(PostgresQueries.createTimestampTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimestampRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_timestamp/");

        assertEquals(row.get("required_field"), Instant.parse("2020-01-01T12:00:00Z"));
        assertEquals(row.get("optional_field"), Instant.parse("2020-06-15T18:30:00Z"));
        assertEquals(row.get("required_array_field"), List.of(
                Instant.parse("2020-01-01T12:00:00Z"),
                Instant.parse("2020-01-02T12:00:00Z")));
        assertEquals(row.get("optional_array_field"), List.of(
                Instant.parse("2020-06-15T18:30:00Z"),
                Instant.parse("2020-06-16T18:30:00Z")));
    }

    @Test
    void testTimestampTzType() {
        var table = "public.test_timestamp_tz";
        postgresHelper.executeSql(PostgresQueries.createTimestampTzTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimestampTzRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_timestamp_tz/");

        assertEquals(row.get("required_field"), Instant.parse("2020-01-01T12:00:00Z"));
        assertEquals(row.get("optional_field"), Instant.parse("2020-06-15T18:30:00Z"));
        assertEquals(row.get("required_array_field"), List.of(
                Instant.parse("2020-01-01T12:00:00Z"),
                Instant.parse("2020-01-02T12:00:00Z")));
        assertEquals(row.get("optional_array_field"), List.of(
                Instant.parse("2020-06-15T18:30:00Z"),
                Instant.parse("2020-06-16T18:30:00Z")));
    }

    @Test
    void testDateType() {
        var table = "public.test_date";
        postgresHelper.executeSql(PostgresQueries.createDateTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertDateRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_date/");

        assertEquals(row.get("required_field"), LocalDate.of(2020, 1, 1));
        assertEquals(row.get("optional_field"), LocalDate.of(2020, 6, 15));
        assertEquals(row.get("required_array_field"), List.of(
                LocalDate.of(2020, 1, 1),
                LocalDate.of(2020, 1, 2)));
        assertEquals(row.get("optional_array_field"), List.of(
                LocalDate.of(2020, 6, 15),
                LocalDate.of(2020, 6, 16)));
    }

    @Test
    void testTimeWithoutTimeZoneType() {
        var table = "public.test_time";
        postgresHelper.executeSql(PostgresQueries.createTimeTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertTimeRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_time/");

        assertEquals(row.get("required_field"), LocalTime.of(12, 34, 56));
        assertEquals(row.get("optional_field"), LocalTime.of(8, 15, 30));
        assertEquals(row.get("required_array_field"), List.of(
                LocalTime.of(12, 0, 0),
                LocalTime.of(13, 0, 0)));
        assertEquals(row.get("optional_array_field"), List.of(
                LocalTime.of(8, 0, 0),
                LocalTime.of(9, 0, 0)));
    }

    // --- BOOLEAN ---

    @Test
    void testBooleanType() {
        var table = "public.test_boolean";
        postgresHelper.executeSql(PostgresQueries.createBooleanTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertBooleanRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_boolean/");

        assertEquals(row.get("required_field"), true);
        assertEquals(row.get("optional_field"), false);
        assertEquals(row.get("required_array_field"), List.of(true, false));
        assertEquals(row.get("optional_array_field"), List.of(false, true));
    }

    // --- UUID ---

    @Test
    void testUuidType() {
        var table = "public.test_uuid";
        postgresHelper.executeSql(PostgresQueries.createUuidTable(table));
        postgresHelper.executeSql(PostgresQueries.addTableToPublication(PUBLICATION, table));
        postgresHelper.executeSql(PostgresQueries.insertUuidRow(table, 1));

        var row = waitAndReadAvroRecord("default/public/test_uuid/");

        assertEquals(row.get("required_field"), UUID.fromString("550e8400-e29b-41d4-a716-446655440000"));
        assertEquals(row.get("optional_field"), UUID.fromString("650e8400-e29b-41d4-a716-446655440000"));
        assertEquals(row.get("required_array_field"), List.of(
                UUID.fromString("550e8400-e29b-41d4-a716-446655440000"),
                UUID.fromString("650e8400-e29b-41d4-a716-446655440000")));
        assertEquals(row.get("optional_array_field"), List.of(
                UUID.fromString("750e8400-e29b-41d4-a716-446655440000"),
                UUID.fromString("850e8400-e29b-41d4-a716-446655440000")));
    }

    private GenericRecord waitAndReadAvroRecord(String prefix) {
        var saver = (AbstractEventSaver<?>) eventSaver;
        await().atMost(Duration.ofSeconds(120)).pollInterval(Duration.ofSeconds(1)).until(() -> saver.getCurrentRecords() > 0);
        eventSaver.flush();

        var keys = minioHelper.listObjectKeys(BUCKET, prefix);
        assertFalse(keys.isEmpty(), "No Avro files found for prefix: " + prefix);
        var bytes = minioHelper.getObjectBytes(BUCKET, keys.getFirst());

        try {
            var datumReader = new GenericDatumReader<GenericRecord>();
            var reader = new DataFileReader<>(new SeekableByteArrayInput(bytes), datumReader);
            return reader.next();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
