package io.debezium.postgres2lake.infrastructure.format.orc;

import org.apache.orc.CompressionKind;

import java.util.Optional;

public enum OrcCompressionCodec {
    NONE(CompressionKind.NONE),
    ZLIB(CompressionKind.ZLIB),
    SNAPPY(CompressionKind.SNAPPY),
    LZO(CompressionKind.LZO),
    LZ4(CompressionKind.LZ4),
    ZSTD(CompressionKind.ZSTD),
    BROTLI(CompressionKind.BROTLI);

    public final CompressionKind codec;

    OrcCompressionCodec(CompressionKind codec) {
        this.codec = codec;
    }

    public static OrcCompressionCodec fromConfig(Optional<String> optional) {
        if (optional.isEmpty() || optional.get().isBlank()) {
            return NONE;
        }
        try {
            return valueOf(optional.get().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown ORC compression codec: " + optional.get(), e);
        }
    }
}
