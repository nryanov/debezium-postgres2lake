package io.debezium.postgres2lake.engine.s3;

import io.debezium.postgres2lake.engine.EventCommitter;
import io.debezium.postgres2lake.engine.EventRecord;

import java.util.stream.Stream;

public interface EventSaver {
    void save(Stream<EventRecord> events, EventCommitter committer);
}
