package io.debezium.postgres2lake.orc.config;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.orc.infrastructure.format.orc.OrcCompressionCodec;
import io.smallrye.config.ConfigMapping;

import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.orc")
public interface OrcConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<OrcCompressionCodec> codec();
}
