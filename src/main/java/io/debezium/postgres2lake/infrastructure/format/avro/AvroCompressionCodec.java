package io.debezium.postgres2lake.infrastructure.format.avro;

import org.apache.avro.file.CodecFactory;

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
}
