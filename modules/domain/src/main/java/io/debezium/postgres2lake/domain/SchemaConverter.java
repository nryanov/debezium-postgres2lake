package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.EventRecord;

public interface SchemaConverter<T> {
    T extractSchema(EventRecord event);

    default boolean isNewSchema(EventRecord event) {
        return true;
    }
}
