package io.debezium.postgres2lake.avro.infrastructure.format.avro;

import org.apache.avro.file.CodecFactory;

import java.util.Optional;

public enum AvroCompressionCodec {
    NONE(CodecFactory.nullCodec()),
    SNAPPY(CodecFactory.snappyCodec()),
    DEFLATE(CodecFactory.deflateCodec(CodecFactory.DEFAULT_DEFLATE_LEVEL)),
    BZIP2(CodecFactory.bzip2Codec()),
    ZSTD(CodecFactory.zstandardCodec(CodecFactory.DEFAULT_ZSTANDARD_LEVEL)),
    XZ(CodecFactory.xzCodec(CodecFactory.DEFAULT_XZ_LEVEL));

    public final CodecFactory codec;

    AvroCompressionCodec(CodecFactory codec) {
        this.codec = codec;
    }

    public static AvroCompressionCodec fromConfig(Optional<String> optional) {
        if (optional.isEmpty() || optional.get().isBlank()) {
            return NONE;
        }
        try {
            return valueOf(optional.get().trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Unknown Avro compression codec: " + optional.get(), e);
        }
    }
}
