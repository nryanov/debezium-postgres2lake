package io.debezium.postgres2lake.domain;

import java.util.stream.Stream;

public interface EventSaver {
    void save(Stream<EventRecord> events, EventCommitter committer);
}
