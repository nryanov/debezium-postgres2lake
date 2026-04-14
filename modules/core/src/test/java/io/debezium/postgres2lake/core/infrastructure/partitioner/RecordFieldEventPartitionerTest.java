package io.debezium.postgres2lake.core.infrastructure.partitioner;

import org.apache.avro.Schema;
import org.apache.avro.util.Utf8;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class RecordFieldEventPartitionerTest {

    private static final String FIELD = "region";

    @Test
    void resolvePartitionStringValue() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, Schema.create(Schema.Type.STRING), "us-east");

        assertEquals(
                "s3a://bucket/db/schema/table/us-east",
                partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartitionUtf8Value() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, Schema.create(Schema.Type.STRING), new Utf8("us-west"));

        assertEquals(
                "s3a://bucket/db/schema/table/us-west",
                partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartitionNonCharSequenceUsesToString() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, Schema.create(Schema.Type.INT), 42);

        assertEquals(
                "s3a://bucket/db/schema/table/42",
                partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartitionThrowsWhenFieldMissing() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithOnlyOtherField();

        assertThrows(IllegalArgumentException.class, () -> partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartitionThrowsWhenFieldNull() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, EventRecordTestSupport.nullableString(), null);

        assertThrows(IllegalArgumentException.class, () -> partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartition_throwsWhenSegmentEmpty() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, Schema.create(Schema.Type.STRING), "");

        assertThrows(IllegalArgumentException.class, () -> partitioner.resolvePartition("s3a://bucket", record));
    }

    @Test
    void resolvePartitionThrowsWhenSegmentContainsSlash() {
        var partitioner = new RecordFieldEventPartitioner(FIELD);
        var record = EventRecordTestSupport.recordWithPartitionField(FIELD, Schema.create(Schema.Type.STRING), "a/b");

        assertThrows(IllegalArgumentException.class, () -> partitioner.resolvePartition("s3a://bucket", record));
    }
}
