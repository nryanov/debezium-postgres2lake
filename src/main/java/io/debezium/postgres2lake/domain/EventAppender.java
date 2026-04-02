package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.TableWriter;

public interface EventAppender<W extends TableWriter> {
    void appendEvent(EventRecord event, W writer) throws Exception;

    void commitPendingEvents(W writer) throws Exception;
}
