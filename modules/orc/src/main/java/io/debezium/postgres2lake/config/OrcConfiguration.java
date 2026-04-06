package io.debezium.postgres2lake.config;

import io.debezium.postgres2lake.infrastructure.format.orc.OrcCompressionCodec;

import java.util.Optional;

public interface OrcConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<OrcCompressionCodec> codec();
}
