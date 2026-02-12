package io.debezium.postgres2lake.engine;

import io.debezium.postgres2lake.common.UnsafeFunction;

public record EventCommitter(UnsafeFunction<Exception> commitBatch, UnsafeFunction<Exception> commitLastRecord) {
    public void commit() {
        try {
            commitLastRecord.run();
            commitBatch.run();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
