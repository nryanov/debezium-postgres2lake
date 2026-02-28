package io.debezium.postgres2lake.engine.s3;

import io.debezium.postgres2lake.engine.EventCommitter;
import io.debezium.postgres2lake.engine.EventRecord;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.data.GenericRow;
import org.apache.paimon.options.Options;
import org.apache.paimon.table.Table;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.StreamTableWrite;
import org.apache.paimon.table.sink.StreamWriteBuilder;
import org.apache.paimon.types.DataTypes;
import org.apache.paimon.types.RowKind;
import org.jboss.logging.Logger;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

@ApplicationScoped
public class S3PaimonEventSaver implements EventSaver {
    private record PaimonWriter(
            Table table,
            StreamWriteBuilder writeBuilder,
            AtomicReference<StreamTableWrite> writer,
            List<CommitMessage> pendingCommits,
            AtomicInteger commitId
    ) {}

    private static final Logger logger = Logger.getLogger(S3PaimonEventSaver.class);

    private final List<EventCommitter> committers;
    private final Map<String, PaimonWriter> openedDescriptors;

    private final Duration timeoutThreshold;
    private final int totalRecordsThreshold;

    private final ScheduledExecutorService scheduledExecutor;

    private int currentRecords;

    private final Catalog catalog;

    public S3PaimonEventSaver() {
        this.committers = new ArrayList<>();
        this.openedDescriptors = new HashMap<>();

        this.timeoutThreshold = Duration.ofMinutes(5);
        this.totalRecordsThreshold = 10;

        this.currentRecords = 0;

        this.scheduledExecutor = Executors.newScheduledThreadPool(1);
        this.scheduledExecutor.scheduleWithFixedDelay(() -> attemptToDumpCurrentData(true), timeoutThreshold.toMillis(), timeoutThreshold.toMillis(), TimeUnit.MILLISECONDS);


        var config = new Configuration();
        config.set("fs.s3a.access.key", "admin");
        config.set("fs.s3a.secret.key", "password");
        config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        config.set("fs.s3a.path.style.access", "true");
        config.set("fs.s3a.endpoint", "http://localhost:9000");
        var options = new Options();
        options.set("type", "jdbc");
        options.set("warehouse", "s3a://warehouse/paimon-warehouse");
        options.set("jdbc-url", "jdbc:postgresql://localhost:5432/postgres");
        options.set("jdbc-user", "postgres");
        options.set("jdbc-password", "postgres");
        options.set("jdbc-driver", "org.postgresql.Driver");
        options.set("jdbc-table-prefix", "paimon_");

        var catalogContext = CatalogContext.create(options, config);
        this.catalog = CatalogFactory.createCatalog(catalogContext);
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
                    saveRecord(event, currentEvents);
                    currentRecords++;
                } catch (Exception e) {
                    logger.errorf(e, "Error happened while adding new avro to parquet writer: %s", e.getLocalizedMessage());
                    throw new RuntimeException(e);
                }
            });

            committers.add(committer);
            logger.infof("Successfully appended records (stream)");
        }
    }

    private void commitChanges(PaimonWriter wrapper) {
        var commit = wrapper.writeBuilder.newCommit();
        commit.commit(wrapper.commitId.get(), wrapper.pendingCommits);
        wrapper.pendingCommits.clear();
    }

    private void saveRecord(EventRecord event, PaimonWriter wrapper) throws Exception {
        var write = wrapper.writer.get();
        if (write == null) {
            write = wrapper.writeBuilder.newWrite();
            wrapper.writer.set(write);
        }

        // todo: fix bucket id resolution
        var bucket = 0;
        write.write(GenericRow.ofKind(RowKind.INSERT, event.getAfter().get("id")), bucket);
        // todo: commit only before saving data
        var pendingCommit = write.prepareCommit(false, wrapper.commitId.incrementAndGet());
        wrapper.pendingCommits.addAll(pendingCommit);
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
                    commitChanges(writer);
                } catch (Exception e) {
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

    private PaimonWriter createWriter(String location, Schema schema) {
        var tableIdentifier = Identifier.create("paimon-development", "data");
        try {
            var paimonSchema = org.apache.paimon.schema.Schema
                    .newBuilder()
                    .column("id", DataTypes.BIGINT())
                    .primaryKey("id")
                    .build();

            catalog.createDatabase("paimon-development", true);
            catalog.createTable(tableIdentifier, paimonSchema, true);
        } catch (Exception e) {
            logger.errorf(e, "Error happened while creating namespace/table: %s", e.getLocalizedMessage());
        }

        try {
            var table = catalog.getTable(tableIdentifier);
            var writerBuilder = table.newStreamWriteBuilder();

            return new PaimonWriter(table, writerBuilder, new AtomicReference<>(), new ArrayList<>(), new AtomicInteger(0));
        } catch (Catalog.TableNotExistException e) {
            throw new RuntimeException(e);
        }
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

