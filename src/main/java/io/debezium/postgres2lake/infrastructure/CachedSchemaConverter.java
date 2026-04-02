package io.debezium.postgres2lake.infrastructure;

import io.debezium.postgres2lake.domain.SchemaConverter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.apache.avro.Schema;

import java.util.WeakHashMap;

public class CachedSchemaConverter<T> implements SchemaConverter<T> {
    private final SchemaConverter<T> delegate;
    private final WeakHashMap<Schema, T> cache;

    public CachedSchemaConverter(SchemaConverter<T> delegate) {
        this.delegate = delegate;
        this.cache = new WeakHashMap<>();
    }

    @Override
    public T extractSchema(EventRecord event) {
        var cachedSchema = cache.get(event.valueSchema());

        if (cachedSchema == null) {
            cachedSchema = delegate.extractSchema(event);
            cache.put(event.valueSchema(), cachedSchema);
        }

        return cachedSchema;
    }
}
