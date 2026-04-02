package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.Schema;

public interface EventAppender {
    void appendEvent(EventRecord event) throws Exception;

    void commitPendingEvents() throws Exception;

    String currentPartition();

    Schema currentSchema();
}
