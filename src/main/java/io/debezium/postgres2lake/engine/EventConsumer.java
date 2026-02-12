package io.debezium.postgres2lake.engine;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.postgres2lake.engine.avro.GenericRecordSerde;
import io.debezium.postgres2lake.engine.s3.S3ParquetEventSaver;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.List;

@ApplicationScoped
public class EventConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private final static Logger logger = Logger.getLogger(EventConsumer.class);

    private final GenericRecordSerde serde;
    private final S3ParquetEventSaver eventSaver;

    public EventConsumer(GenericRecordSerde serde, S3ParquetEventSaver eventSaver) {
        this.serde = serde;
        this.eventSaver = eventSaver;
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        logger.infof("Processing next batch: %s", records.size());

        // to allow GC free space
        var fullClone = records.getLast();

        var eventCommitter = new EventCommitter(committer::markBatchFinished, () -> committer.markProcessed(fullClone));
        var stream = records.stream().map(serde::deserialize);

        eventSaver.save(stream, eventCommitter);
        logger.infof("Successfully handled batch with `%s` records", records.size());
    }
}
