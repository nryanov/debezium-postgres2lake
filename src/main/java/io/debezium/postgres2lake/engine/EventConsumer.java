package io.debezium.postgres2lake.engine;

import io.debezium.engine.ChangeEvent;
import io.debezium.engine.DebeziumEngine;
import io.debezium.postgres2lake.engine.avro.GenericRecordSerde;
import jakarta.enterprise.context.ApplicationScoped;
import org.jboss.logging.Logger;

import java.util.List;

@ApplicationScoped
public class EventConsumer implements DebeziumEngine.ChangeConsumer<ChangeEvent<Object, Object>> {
    private final static Logger logger = Logger.getLogger(EventConsumer.class);

    private final GenericRecordSerde serde;

    public EventConsumer(GenericRecordSerde serde) {
        this.serde = serde;
    }

    @Override
    public void handleBatch(List<ChangeEvent<Object, Object>> records, DebeziumEngine.RecordCommitter<ChangeEvent<Object, Object>> committer) throws InterruptedException {
        records.forEach(record -> {
            try {
                var event = serde.deserialize(record);
                logger.infof("Got event: %s", event);
                committer.markProcessed(record);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });

        committer.markBatchFinished();
    }
}
