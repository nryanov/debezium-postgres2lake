package io.debezium.postgres2lake.orc.infrastructure.appender;

import io.debezium.postgres2lake.orc.infrastructure.OrcTableWriter;

public class DefaultOrcEventAppenderFactory implements OrcEventAppenderFactory {
    @Override
    public OrcEventAppender create(OrcTableWriter writer) {
        return new OrcEventAppender(writer);
    }
}
