package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventRecord;

public interface EventAppender<W> {
    void appendEvent(EventRecord event, W writer) throws Exception;

    void commitPendingEvents(W writer) throws Exception;
}
