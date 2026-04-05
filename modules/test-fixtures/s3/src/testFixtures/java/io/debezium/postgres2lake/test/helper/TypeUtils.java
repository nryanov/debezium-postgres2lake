package io.debezium.postgres2lake.test.helper;

import org.apache.avro.util.Utf8;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

public abstract class TypeUtils {
    public static Object readUuidOrUtf8(byte[] bytes) {
        var raw = new String(bytes, StandardCharsets.UTF_8);
        if (raw.length() == 36 && raw.charAt(8) == '-') {
            try {
                return UUID.fromString(raw);
            } catch (IllegalArgumentException ignored) {
                // ignore
            }
        }
        // Avro UUID logical type often stored as 16 raw bytes; avoid misreading 16-char text as random UUID bytes.
        if (bytes.length == 16 && maybeUuid(bytes)) {
            return uuidFrom16Bytes(bytes);
        }
        return new Utf8(raw);
    }

    public static Object readUuidOrString(byte[] bytes) {
        var raw = new String(bytes, StandardCharsets.UTF_8);
        if (raw.length() == 36 && raw.charAt(8) == '-') {
            try {
                return UUID.fromString(raw);
            } catch (IllegalArgumentException ignored) {
                // ignore
            }
        }
        // Avro UUID logical type often stored as 16 raw bytes; avoid misreading 16-char text as random UUID bytes.
        if (bytes.length == 16 && maybeUuid(bytes)) {
            return uuidFrom16Bytes(bytes);
        }
        return raw;
    }

    public static boolean maybeUuid(byte[] b) {
        if (b.length != 16) {
            return false;
        }
        // IETF variant + UUID version 4 (matches fixtures in PostgresQueries.insertUuidRow)
        return (b[8] & 0xc0) == 0x80 && (b[6] & 0xf0) == 0x40;
    }

    public static Object readUuidOrUtf8Bytes(byte[] bytes) {
        if (bytes.length == 16) {
            return uuidFrom16Bytes(bytes);
        }
        return new Utf8(new String(bytes, StandardCharsets.UTF_8));
    }

    public static Object readUuidOrBytes(byte[] bytes) {
        if (bytes.length == 16) {
            return uuidFrom16Bytes(bytes);
        }
        return new Utf8(new String(bytes, StandardCharsets.UTF_8));
    }

    public static UUID uuidFrom16Bytes(byte[] bytes) {
        var buffer = ByteBuffer.wrap(bytes);
        var msb = buffer.getLong();
        var lsb = buffer.getLong();
        return new UUID(msb, lsb);
    }
}
