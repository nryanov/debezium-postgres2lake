package io.debezium.postgres2lake.avro.config;

import io.debezium.postgres2lake.avro.infrastructure.format.avro.AvroCompressionCodec;
import io.smallrye.config.ConfigMapping;

import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.avro")
public interface AvroConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<AvroCompressionCodec> codec();
}
