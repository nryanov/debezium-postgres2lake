package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventCommitter;
import io.debezium.postgres2lake.domain.model.EventRecord;

import java.util.stream.Stream;

public interface EventSaver {
    void save(Stream<EventRecord> events, EventCommitter committer);

    void close();

    void flush();

    // for testing purposes only
    int getCurrentRecords();
}
