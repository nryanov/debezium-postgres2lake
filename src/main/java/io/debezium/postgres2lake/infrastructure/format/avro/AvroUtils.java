package io.debezium.postgres2lake.infrastructure.format.avro;

import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;

public abstract class AvroUtils {
    private static final DateTimeFormatter ISO_OFFSET_DATE_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE;
    private static final DateTimeFormatter ISO_OFFSET_TIME_FORMAT = DateTimeFormatter.ISO_OFFSET_TIME;
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    public static byte[] convertToBytes(Object value) {
        return switch (value) {
            case ByteBuffer buffer -> {
                byte[] bytes = new byte[buffer.remaining()];
                buffer.duplicate().get(bytes);
                yield bytes;
            }
            case byte[] bytes -> bytes;
            case GenericData.Fixed fixed -> fixed.bytes();
            default -> String.valueOf(value).getBytes(StandardCharsets.UTF_8);
        };
    }

    public static String convertToString(Object value) {
        return switch (value) {
            case Utf8 utf8 -> utf8.toString();
            default -> value.toString();
        };
    }

    public static LocalDate parseIsoDate(Object avroValue) {
        var rawDate = convertToString(avroValue);
        // ISO format in debezium in UTC format
        return LocalDate.parse(rawDate, ISO_OFFSET_DATE_FORMAT);
    }

    public static OffsetDateTime parseIsoTimestamp(Object avroValue) {
        var rawTimestamp = convertToString(avroValue);
        // ISO format in debezium in UTC format
        return OffsetDateTime.parse(rawTimestamp, ISO_TIMESTAMP_FORMAT);
    }

    public static OffsetTime parseIsoTime(Object avroValue) {
        var rawTime = convertToString(avroValue);
        // ISO format in debezium in UTC format
        return OffsetTime.parse(rawTime, ISO_OFFSET_TIME_FORMAT);
    }
}
