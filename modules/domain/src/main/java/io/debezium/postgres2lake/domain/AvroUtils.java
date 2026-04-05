package io.debezium.postgres2lake.domain;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

public abstract class AvroUtils {
    private static final DateTimeFormatter ISO_OFFSET_DATE_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE;
    private static final DateTimeFormatter ISO_OFFSET_TIME_FORMAT = DateTimeFormatter.ISO_OFFSET_TIME;
    private static final DateTimeFormatter ISO_TIMESTAMP_FORMAT = DateTimeFormatter.ISO_OFFSET_DATE_TIME;

    private static final UUID ZERO_UUID = UUID.fromString("00000000-0000-0000-0000-000000000000");

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

    public static UUID convertToUuid(Object value) {
        var rawUuid = convertToString(value);

        if (rawUuid.isEmpty()) {
            // for required UUID. Otherwise in case of DELETE operation postgres may return empty string
            return ZERO_UUID;
        }

        return UUID.fromString(rawUuid);
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

    public static Schema unwrapUnion(Schema schema) {
        if (schema.getType() == Schema.Type.UNION) {
            for (var type : schema.getTypes()) {
                // return first non-null paimonSchema
                if (type.getType() != Schema.Type.NULL) {
                    return type;
                }
            }
        }

        return schema;
    }
}
