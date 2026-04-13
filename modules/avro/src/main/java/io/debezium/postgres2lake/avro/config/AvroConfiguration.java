package io.debezium.postgres2lake.avro.config;

import io.debezium.postgres2lake.avro.infrastructure.AvroCompressionCodec;
import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.smallrye.config.ConfigMapping;

import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.avro")
public interface AvroConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<AvroCompressionCodec> codec();
}
