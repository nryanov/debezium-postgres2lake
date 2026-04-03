package io.debezium.postgres2lake.infrastructure.format.paimon;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

public record PaimonTableWriter(
        Identifier identifier,
        Table table,
        Schema paimonSchema,
        org.apache.avro.Schema avroSchema,
        StreamWriteBuilder writeBuilder,
        AtomicReference<StreamTableWrite> writer,
        List<CommitMessage> pendingCommits,
        AtomicInteger commitId
) {
}
