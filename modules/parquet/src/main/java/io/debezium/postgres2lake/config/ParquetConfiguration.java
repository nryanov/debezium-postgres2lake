package io.debezium.postgres2lake.config;

import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;

import java.util.Optional;

public interface ParquetConfiguration {
    CommonConfiguration.Threshold threshold();

    CommonConfiguration.FileIO fileIO();

    CommonConfiguration.OutputNamingStrategy namingStrategy();

    Optional<ParquetCompressionCodec> codec();
}
