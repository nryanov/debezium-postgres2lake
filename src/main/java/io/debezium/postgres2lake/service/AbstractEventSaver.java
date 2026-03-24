package io.debezium.postgres2lake.service;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.EventCommitter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import org.jboss.logging.Logger;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

abstract public class AbstractEventSaver<T> implements EventSaver {
    private static final Logger logger = Logger.getLogger(AbstractEventSaver.class);

    private final List<EventCommitter> committers;
    private final Map<String, T> openedDescriptors;
    private final ScheduledExecutorService scheduledExecutor;
    private final int totalRecordsThreshold;

    private int currentRecords;

    public AbstractEventSaver(OutputConfiguration.Threshold threshold) {
        this.committers = new ArrayList<>();
        this.openedDescriptors = new HashMap<>();

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
                    var destination = event.rawDestination();
                    var openedDescriptor = openedDescriptors.computeIfAbsent(destination, ignored -> createWriter(event));
                    appendEvent(event, openedDescriptor);
                    currentRecords++;
                }
            } catch (Exception e) {
                logger.errorf(e, "Error happened while handle new events batch: %s", e.getLocalizedMessage());
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
                for (var entry : openedDescriptors.entrySet()) {
                    var writer = entry.getValue();
                    commitPendingEvents(writer);
                }

                // commit every hold batch
                committers.forEach(EventCommitter::commit);
                logger.infof("Successfully saved %s total records", currentRecords);

                openedDescriptors.clear();
                committers.clear();
                currentRecords = 0;
                logger.infof("Successfully reset records backlog");
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract T createWriter(EventRecord event);

    protected abstract void appendEvent(EventRecord event, T writer) throws Exception;

    protected abstract void commitPendingEvents(T writer) throws Exception;

    // for testing purposes only
    public int getCurrentRecords() {
        return currentRecords;
    }
}
