package io.debezium.postgres2lake.infrastructure.format.paimon;

import io.debezium.postgres2lake.domain.model.PartitionAware;
import io.debezium.postgres2lake.domain.model.SchemaAware;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public record PaimonWriter(
        Table table,
        Schema paimonSchema,
        org.apache.avro.Schema avroSchema,
        StreamWriteBuilder writeBuilder,
        AtomicReference<StreamTableWrite> writer,
        List<CommitMessage> pendingCommits,
        AtomicInteger commitId
) implements SchemaAware, PartitionAware {
    @Override
    public String partition() {
        // paimon resolve partition in StreamWriteBuilder
        return "";
    }

    @Override
    public org.apache.avro.Schema schema() {
        return avroSchema;
    }
}
