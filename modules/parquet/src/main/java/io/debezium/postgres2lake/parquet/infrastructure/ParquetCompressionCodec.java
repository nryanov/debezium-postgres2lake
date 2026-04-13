package io.debezium.postgres2lake.parquet.infrastructure;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.util.Optional;

public enum ParquetCompressionCodec {
    NONE(CompressionCodecName.UNCOMPRESSED),
    SNAPPY(CompressionCodecName.SNAPPY),
    GZIP(CompressionCodecName.GZIP),
    LZO(CompressionCodecName.LZO),
    BROTLI(CompressionCodecName.BROTLI),
    LZ4(CompressionCodecName.LZ4),
    ZSTD(CompressionCodecName.ZSTD);

    public final CompressionCodecName codecName;

    ParquetCompressionCodec(CompressionCodecName codecName) {
        this.codecName = codecName;
    }

    public static ParquetCompressionCodec fromConfig(Optional<String> optional) {
        if (optional.isEmpty() || optional.get().isBlank()) {
            return NONE;
        }
        try {
            return valueOf(optional.get().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown Parquet compression codec: " + optional.get(), e);
        }
    }
}
