package io.debezium.postgres2lake.engine.s3;

import io.debezium.postgres2lake.engine.EventCommitter;
import io.debezium.postgres2lake.engine.EventRecord;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.Schema;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.data.GenericAppenderFactory;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.io.BaseTaskWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.io.UnpartitionedWriter;
import org.apache.iceberg.types.TypeUtil;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT;
import static org.apache.iceberg.TableProperties.DEFAULT_FILE_FORMAT_DEFAULT;

@ApplicationScoped
public class S3IcebergEventSaver implements EventSaver {
    private static final Logger logger = Logger.getLogger(S3IcebergEventSaver.class);

    private final List<EventCommitter> committers;
    private final Map<String, Object> openedDescriptors;

    private final Duration timeoutThreshold;
    private final int totalRecordsThreshold;

    private final ScheduledExecutorService scheduledExecutor;

    private int currentRecords;

    private final Catalog catalog;

    public S3IcebergEventSaver() {
        this.committers = new ArrayList<>();
        this.openedDescriptors = new HashMap<>();

        this.timeoutThreshold = Duration.ofMinutes(5);
        this.totalRecordsThreshold = 10;

        this.currentRecords = 0;

        this.scheduledExecutor = Executors.newScheduledThreadPool(1);
        this.scheduledExecutor.scheduleWithFixedDelay(() -> attemptToDumpCurrentData(true), timeoutThreshold.toMillis(), timeoutThreshold.toMillis(), TimeUnit.MILLISECONDS);

        this.catalog = null;
    }

    @Override
    public void save(Stream<EventRecord> events, EventCommitter committer) {
        attemptToDumpCurrentData(false);
        backlogData(events, committer);
    }

    @SuppressWarnings({"unchecked", "resource"})
    private void backlogData(Stream<EventRecord> events, EventCommitter committer) {
        synchronized (this) {
            logger.info("Append records (stream)");
            events.forEach(event -> {
                var destination = event.destination();
                var location = generateLocation("warehouse", event.destination());
                var currentEvents = openedDescriptors.computeIfAbsent(destination, ignored -> createWriter(location, event.value().getSchema()));

                try {
                    currentEvents.append(event.value());
                    currentRecords++;
                } catch (IOException e) {
                    logger.errorf(e, "Error happened while adding new avro to parquet writer: %s", e.getLocalizedMessage());
                    throw new RuntimeException(e);
                }
            });

            committers.add(committer);
            logger.infof("Successfully appended records (stream)");
        }
    }

    private void attemptToDumpCurrentData(boolean byTime) {
        synchronized (this) {
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
                try {
                    var writer = entry.getValue();
                    writer.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }

            // commit every hold batch
            committers.forEach(EventCommitter::commit);
            logger.infof("Successfully saved %s total records", currentRecords);

            openedDescriptors.clear();
            committers.clear();
            currentRecords = 0;
            logger.infof("Successfully reset records backlog");
        }
    }

    private Object createWriter(String location, Schema schema) {
        var table = catalog.loadTable(null);
        var writer = createTableWriter(table);

        return null;
    }

    private BaseTaskWriter<Record> createTableWriter(Table table) {
        var fileFormat = resolveTableFormat(table);
        var appenderFactory = createTableAppender(table);
        var outputFileFactory = createTableOutputFileFactory(table, fileFormat);

        var fileSize = 10 * 1024 * 1024; // todo: get from config

        // todo: resolve partitioned or unpartitioned writer
        new UnpartitionedWriter<>(
                table.spec(), fileFormat, appenderFactory, outputFileFactory, table.io(), fileSize);
    }

    private FileFormat resolveTableFormat(Table table) {
        String formatAsString = table.properties().getOrDefault(DEFAULT_FILE_FORMAT, DEFAULT_FILE_FORMAT_DEFAULT);
        return FileFormat.valueOf(formatAsString.toUpperCase(Locale.ROOT));
    }

    public static GenericAppenderFactory createTableAppender(Table table) {
        final Set<Integer> identifierFieldIds = table.schema().identifierFieldIds();
        if (identifierFieldIds == null || identifierFieldIds.isEmpty()) {
            return new GenericAppenderFactory(
                    table.schema(),
                    table.spec(),
                    null,
                    null,
                    null)
                    .setAll(table.properties());
        } else {
            return new GenericAppenderFactory(
                    table.schema(),
                    table.spec(),
                    identifierFieldIds.stream().mapToInt(it -> it).toArray(),
                    TypeUtil.select(table.schema(), new HashSet<>(identifierFieldIds)),
                    null)
                    .setAll(table.properties());
        }
    }

    private OutputFileFactory createTableOutputFileFactory(Table table, FileFormat format) {
        var partitionId = 1; // todo: generate partition id
        var taskId = 1L; // todo: generate task id

        return OutputFileFactory.builderFor(table, partitionId, taskId)
                .defaultSpec(table.spec())
                .operationId(UUID.randomUUID().toString())
                .format(format)
                .build();
    }

    private String generateLocation(String bucket, String destination) {
        return String.format(
                "s3a://%s/%s/%s_%s.avro",
                bucket,
                destination, // todo: -> schema/table/[file1, file2, .., fileN]
                destination,
                System.currentTimeMillis()
        );
    }
}
