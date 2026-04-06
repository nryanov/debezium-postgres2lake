package io.debezium.postgres2lake.config;

import java.util.Map;

public interface PaimonConfiguration {
    CommonConfiguration.Threshold threshold();

    Map<String, String> properties();

    CommonConfiguration.FileIO fileIO();
}
