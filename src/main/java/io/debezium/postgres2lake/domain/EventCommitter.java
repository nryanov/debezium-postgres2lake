package io.debezium.postgres2lake.domain;


import io.debezium.postgres2lake.domain.common.UnsafeFunction;

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
