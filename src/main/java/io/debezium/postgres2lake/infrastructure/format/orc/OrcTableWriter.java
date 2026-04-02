package io.debezium.postgres2lake.infrastructure.format.orc;

import io.debezium.postgres2lake.domain.model.PartitionAware;
import io.debezium.postgres2lake.domain.model.SchemaAware;
import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

public record OrcTableWriter(
        Writer writer,
        VectorizedRowBatch batch,
        Schema schema,
        String partition
) implements SchemaAware, PartitionAware {
}
