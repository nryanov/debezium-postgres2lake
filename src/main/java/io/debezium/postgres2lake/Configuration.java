package io.debezium.postgres2lake;

import io.smallrye.config.ConfigMapping;

@ConfigMapping(prefix = "debezium.postgres2lake")
public interface Configuration {

    interface OffsetStorage {}

    interface DatabaseConnection {}

    interface S3Connection {}

    interface Replication {}

    interface FlushThreshold {}

    interface SystemTables {
        interface Heartbeat {}

        interface Signal {}
    }
}
