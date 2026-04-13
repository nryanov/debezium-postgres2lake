package io.debezium.postgres2lake.orc.infrastructure.appender;

import io.debezium.postgres2lake.orc.infrastructure.OrcTableWriter;

public interface OrcEventAppenderFactory {
    OrcEventAppender create(OrcTableWriter writer);
}
