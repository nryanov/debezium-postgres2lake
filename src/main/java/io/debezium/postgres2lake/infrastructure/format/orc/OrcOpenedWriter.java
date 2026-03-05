package io.debezium.postgres2lake.infrastructure.format.orc;

import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.Writer;

public record OrcOpenedWriter(Writer writer, VectorizedRowBatch batch) {
}
