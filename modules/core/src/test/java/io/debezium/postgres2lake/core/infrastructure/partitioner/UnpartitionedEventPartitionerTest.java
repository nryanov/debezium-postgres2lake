package io.debezium.postgres2lake.core.infrastructure.partitioner;

import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class UnpartitionedEventPartitionerTest {

    @Test
    void resolvePartition_buildsPathWithoutDateSegment() {
        var partitioner = new UnpartitionedEventPartitioner();
        var empty = new GenericData.Record(Schema.createRecord("value", null, null, false, List.of()));
        var record = new EventRecord(Operation.INSERT, empty, empty, EventRecordTestSupport.RAW_DESTINATION);

        assertEquals(
                "s3a://warehouse/db/schema/table",
                partitioner.resolvePartition("s3a://warehouse", record));
    }

    @Test
    void resolvePartitionSupportsHdfsRootPath() {
        var partitioner = new UnpartitionedEventPartitioner();
        var empty = new GenericData.Record(Schema.createRecord("value", null, null, false, List.of()));
        var record = new EventRecord(Operation.INSERT, empty, empty, EventRecordTestSupport.RAW_DESTINATION);

        assertEquals(
                "hdfs://namenode:8020/lake/db/schema/table",
                partitioner.resolvePartition("hdfs://namenode:8020/lake", record));
    }
}
