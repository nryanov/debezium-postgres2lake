package io.debezium.postgres2lake.infrastructure.format.parquet;

import org.apache.parquet.hadoop.metadata.CompressionCodecName;

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
}
