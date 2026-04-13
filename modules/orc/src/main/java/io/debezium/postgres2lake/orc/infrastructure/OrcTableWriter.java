package io.debezium.postgres2lake.orc.infrastructure;

import org.apache.avro.Schema;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

public record OrcTableWriter(
        Writer writer,
        VectorizedRowBatch batch,
        Schema schema,
        String partition
) {
}
