package io.debezium.postgres2lake.iceberg.config;

import io.smallrye.config.ConfigMapping;

import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "debezium.output.iceberg")
public interface IcebergConfiguration {
    CommonConfiguration.Threshold threshold();

    String name();

    Map<String, String> properties();

    CommonConfiguration.FileIO fileIO();

    Map<String, IcebergTableSpec> tableSpecs();

    interface IcebergTableSpec {
        Optional<String> location();

        Map<String, String> properties();

        List<String> partitionBy();

        List<String> sortBy();
    }
}
