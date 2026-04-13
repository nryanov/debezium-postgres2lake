package io.debezium.postgres2lake.parquet.config;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.parquet.infrastructure.ParquetCompressionCodec;
import io.smallrye.config.ConfigMapping;

import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.parquet")
public interface ParquetConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<ParquetCompressionCodec> codec();
}
