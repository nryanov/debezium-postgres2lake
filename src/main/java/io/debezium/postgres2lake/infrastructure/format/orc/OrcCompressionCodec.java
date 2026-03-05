package io.debezium.postgres2lake.infrastructure.format.orc;

import org.apache.orc.CompressionKind;

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
}
