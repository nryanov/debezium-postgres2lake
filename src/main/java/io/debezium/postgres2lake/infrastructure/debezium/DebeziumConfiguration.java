package io.debezium.postgres2lake.infrastructure.debezium;

import io.smallrye.config.ConfigMapping;

import java.util.Map;

@ConfigMapping(prefix = "debezium")
public interface DebeziumConfiguration {
    Map<String, String> engine();

    AvroFormat avro();

    enum AvroFormat {
        CONFLUENT, BINARY
    }
}
