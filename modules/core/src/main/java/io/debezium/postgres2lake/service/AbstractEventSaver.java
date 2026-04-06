package io.debezium.postgres2lake.service;

import io.debezium.postgres2lake.config.CommonConfiguration;
import io.debezium.postgres2lake.domain.EventAppender;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.EventCommitter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.service.exceptions.EventAppendException;
import io.debezium.postgres2lake.service.exceptions.EventFlushException;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

abstract public class AbstractEventSaver<T extends EventAppender> implements EventSaver {
    private static final Logger logger = Logger.getLogger(AbstractEventSaver.class);

    private final List<EventCommitter> committers;
    private final Map<String, T> openedEventAppender;
    private final ScheduledExecutorService scheduledExecutor;
    private final int totalRecordsThreshold;

    private int currentRecords;

    public AbstractEventSaver(CommonConfiguration.Threshold threshold) {
        this.committers = new ArrayList<>();
        this.openedEventAppender = new HashMap<>();

        var timeoutThreshold = threshold.time();
        this.totalRecordsThreshold = threshold.records();
        this.currentRecords = 0;

        this.scheduledExecutor = Executors.newScheduledThreadPool(1);
        this.scheduledExecutor.scheduleWithFixedDelay(() -> attemptToDumpCurrentData(true), timeoutThreshold.toMillis(), timeoutThreshold.toMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public void save(Stream<EventRecord> events, EventCommitter committer) {
        attemptToDumpCurrentData(false);
        backlogData(events, committer);
    }

    @Override
    public void close() {
        flush();
        scheduledExecutor.close();
    }

    @Override
    public void flush() {
        // force flush
        attemptToDumpCurrentData(true);
    }

    @SuppressWarnings({"unchecked", "resource"})
    private void backlogData(Stream<EventRecord> events, EventCommitter committer) {
        synchronized (this) {
            logger.debug("Append events");
            try {
                var eventsIter = events.iterator();
                while (eventsIter.hasNext()) {
                    var event = eventsIter.next();
                    var appender = getOrCreateEventAppender(event);
                    appender.appendEvent(event);
                    currentRecords++;
                }
            } catch (Exception e) {
                logger.errorf(e, "Error happened while handle new events batch: %s", e.getLocalizedMessage());
                throw new EventAppendException("Failed to handle new events batch", e);
            }

            committers.add(committer);
            logger.debug("Successfully appended events");
        }
    }

    private void attemptToDumpCurrentData(boolean byTime) {
        synchronized (this) {
            try {
                if (!byTime && currentRecords < totalRecordsThreshold) {
                    return;
                }

                if (byTime) {
                    logger.infof("Dump current events by time");
                } else {
                    logger.infof("Dump current events by exceeded records threshold");
                }

                // save events
                for (var entry : openedEventAppender.entrySet()) {
                    var appender = entry.getValue();
                    appender.commitPendingEvents();
                }

                // commit every hold batch
                committers.forEach(EventCommitter::commit);
                logger.infof("Successfully saved %s total records", currentRecords);

                openedEventAppender.clear();
                committers.clear();
                currentRecords = 0;
                logger.infof("Successfully reset records backlog");
            } catch (Exception e) {
                logger.errorf(e, "Error happened while flushing pending events: %s", e.getLocalizedMessage());
                throw new EventFlushException("Failed to flush pending events or commit offsets", e);
            }
        }
    }

    private T getOrCreateEventAppender(EventRecord event) throws Exception {
        var destination = event.rawDestination();
        var currentAppender = openedEventAppender.get(destination);

        if (currentAppender == null) {
            currentAppender = createEventAppender(event);
            openedEventAppender.put(destination, currentAppender);
        } else {
            // check paimonSchema changes & partition rollover
            var currentSchema = currentAppender.currentSchema();
            var eventSchema = event.valueSchema();

            var currentPartition = currentAppender.currentPartition();
            var eventPartition = resolvePartition(event);

            var anyChanges = false;

            if (!currentSchema.equals(eventSchema)) {
                logger.infof("Detect schema change for source %s", destination);
                currentAppender.commitPendingEvents();
                handleSchemaChanges(currentAppender, event);
                anyChanges = true;
            } else if (!currentPartition.equals(eventPartition)) {
                logger.infof("Detect partition rollover for source %s", destination);
                currentAppender.commitPendingEvents();
                anyChanges = true;
            }

            if (anyChanges) {
                currentAppender = createEventAppender(event);
                openedEventAppender.put(destination, currentAppender);
            }
        }

        return currentAppender;
    }

    protected void handleSchemaChanges(T appender, EventRecord event) {}

    protected String resolvePartition(EventRecord event) {
        // if method is not overrided then it means that current eventSaver resolve partition by its own fileIO (e.g. iceberg, paimon)
        return "";
    }

    protected abstract T createEventAppender(EventRecord event);

    // for testing purposes only
    public int getCurrentRecords() {
        return currentRecords;
    }
}
