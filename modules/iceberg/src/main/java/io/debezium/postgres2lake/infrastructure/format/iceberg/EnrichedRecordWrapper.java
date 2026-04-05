package io.debezium.postgres2lake.infrastructure.format.iceberg;

import io.debezium.postgres2lake.domain.model.Operation;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;

import java.util.Map;

public class EnrichedRecordWrapper implements Record {
    private final Record delegate;
    private final Operation operation;

    public EnrichedRecordWrapper(Record delegate, Operation operation) {
        this.delegate = delegate;
        this.operation = operation;
    }

    public Operation operation() {
        return operation;
    }

    @Override
    public Types.StructType struct() {
        return delegate.struct();
    }

    @Override
    public Object getField(String name) {
        return delegate.getField(name);
    }

    @Override
    public void setField(String name, Object value) {
        delegate.setField(name, value);
    }

    @Override
    public Object get(int pos) {
        return delegate.get(pos);
    }

    @Override
    public Record copy() {
        return new EnrichedRecordWrapper(delegate.copy(), operation);
    }

    @Override
    public Record copy(Map<String, Object> overwriteValues) {
        return new EnrichedRecordWrapper(delegate.copy(overwriteValues), operation);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public <T> T get(int pos, Class<T> javaClass) {
        return delegate.get(pos, javaClass);
    }

    @Override
    public <T> void set(int pos, T value) {
        delegate.set(pos, value);
    }

    @Override
    public String toString() {
        return delegate.toString();
    }
}
