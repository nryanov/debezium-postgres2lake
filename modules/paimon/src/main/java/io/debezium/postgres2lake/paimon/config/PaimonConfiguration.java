package io.debezium.postgres2lake.paimon.config;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "debezium.output.paimon")
public interface PaimonConfiguration {
    CommonConfiguration.Threshold threshold();

    Map<String, String> properties();

    CommonConfiguration.FileIO fileIO();
}
