package io.debezium.postgres2lake.core.service;

import io.debezium.postgres2lake.core.config.CommonConfiguration;
import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.model.EventCommitter;
import io.debezium.postgres2lake.domain.model.EventDestination;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.model.Operation;
import io.debezium.postgres2lake.extensions.common.model.TableDestination;
import io.debezium.postgres2lake.extensions.readiness.marker.event.emitter.api.ReadinessMarkerEventEmitterHandler;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AbstractEventSaverReadinessMarkerTest {

    @Test
    void flushEmitsReadinessAfterCommitters() {
        var executionOrder = new ArrayList<String>();
        var readinessHandler = new RecordingReadinessMarkerHandler(executionOrder);
        var saver = new TestEventSaver(readinessHandler);
        var record = testRecord("db.schema.table");
        var committer = new EventCommitter(
                () -> executionOrder.add("batchCommit"),
                () -> executionOrder.add("lastRecordCommit")
        );

        saver.save(Stream.of(record), committer);
        saver.flush();
        saver.close();

        assertEquals(List.of("appenderCommit", "lastRecordCommit", "batchCommit", "readinessEmit"), executionOrder);
        assertEquals(List.of(new TableDestination("db", "schema", "table")), readinessHandler.lastDestinations);
    }

    @Test
    void flushWithoutBufferedDataDoesNotEmitReadiness() {
        var executionOrder = new ArrayList<String>();
        var readinessHandler = new RecordingReadinessMarkerHandler(executionOrder);
        var saver = new TestEventSaver(readinessHandler);

        saver.flush();
        saver.close();

        assertEquals(List.of(), readinessHandler.lastDestinations);
    }

    private static EventRecord testRecord(String destination) {
        var keySchema = Schema.createRecord("key", null, null, false, List.of());
        var valueSchema = Schema.createRecord("value", null, null, false, List.of(
                new Schema.Field("id", Schema.create(Schema.Type.LONG), null, null)
        ));

        GenericRecord key = new GenericData.Record(keySchema);
        GenericRecord value = new GenericData.Record(valueSchema);
        value.put("id", 1L);

        return new EventRecord(Operation.INSERT, key, value, destination);
    }

    private static final class TestEventSaver extends AbstractEventSaver<TestEventAppender> {
        private final List<String> executionOrder;

        private TestEventSaver(RecordingReadinessMarkerHandler readinessMarkerEventEmitterHandler) {
            super(new CommonConfiguration.Threshold() {
                @Override
                public int records() {
                    return 1000;
                }

                @Override
                public Duration time() {
                    return Duration.ofDays(1);
                }
            }, readinessMarkerEventEmitterHandler);
            this.executionOrder = readinessMarkerEventEmitterHandler.executionOrder;
        }

        @Override
        protected TestEventAppender createEventAppender(EventRecord event) {
            return new TestEventAppender(event.valueSchema(), executionOrder);
        }
    }

    private record TestEventAppender(Schema schema, List<String> executionOrder) implements EventAppender {

        @Override
        public void appendEvent(EventRecord event) {
            // no-op
        }

        @Override
        public void commitPendingEvents() {
            executionOrder.add("appenderCommit");
        }

        @Override
        public String currentPartition() {
            return "";
        }

        @Override
        public Schema currentSchema() {
            return schema;
        }

        @Override
        public EventDestination destination() {
            return null;
        }
    }

    private static final class RecordingReadinessMarkerHandler implements ReadinessMarkerEventEmitterHandler {
        private final List<String> executionOrder;
        private List<TableDestination> lastDestinations = List.of();

        private RecordingReadinessMarkerHandler(List<String> executionOrder) {
            this.executionOrder = executionOrder;
        }

        @Override
        public void initialize(Map<String, String> properties) {
            // no-op
        }

        @Override
        public void emit(List<TableDestination> destinations) {
            executionOrder.add("readinessEmit");
            lastDestinations = destinations;
        }

        @Override
        public void close() {
            // no-op
        }
    }
}
