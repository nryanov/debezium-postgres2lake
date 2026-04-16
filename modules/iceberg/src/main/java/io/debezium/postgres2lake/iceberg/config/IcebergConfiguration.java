package io.debezium.postgres2lake.iceberg.config;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.smallrye.config.ConfigMapping;
import io.smallrye.config.WithName;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.iceberg")
public interface IcebergConfiguration {
    CommonConfiguration.Threshold threshold();

    String name();

    Map<String, String> properties();

    CommonConfiguration.FileIO fileIO();

    @WithName("tableSpecs")
    Map<String, IcebergTableSpec> tableSpecs();

    interface IcebergTableSpec {
        Optional<String> location();

        Map<String, String> properties();

        Optional<List<String>> partitionBy();

        Optional<List<String>> sortBy();
    }
}
