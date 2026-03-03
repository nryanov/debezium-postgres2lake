package io.debezium.postgres2lake.infrastructure.s3;

import io.debezium.postgres2lake.domain.model.EventCommitter;
import io.debezium.postgres2lake.domain.model.EventRecord;
import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

public class S3AvroEventSaver implements EventSaver {
    private static final Logger logger = Logger.getLogger(S3AvroEventSaver.class);

    private final OutputLocationGenerator outputLocationGenerator;
    private final List<EventCommitter> committers;
    private final Map<String, DataFileWriter> openedDescriptors;

    private final Duration timeoutThreshold;
    private final int totalRecordsThreshold;

    private final ScheduledExecutorService scheduledExecutor;

    private int currentRecords;

    public S3AvroEventSaver(OutputLocationGenerator outputLocationGenerator) {
        this.outputLocationGenerator = outputLocationGenerator;
        this.committers = new ArrayList<>();
        this.openedDescriptors = new HashMap<>();

        this.timeoutThreshold = Duration.ofMinutes(5);
        this.totalRecordsThreshold = 10;

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
            logger.info("Append records (stream)");
            events.forEach(event -> {
                var destination = event.rawDestination();
                var location = outputLocationGenerator.generateLocation("warehouse", event);
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

    private DataFileWriter<GenericRecord> createWriter(String location, Schema schema) {
        try {
            logger.infof("Opening parquet writer for `%s`", location);
            var path = new Path(new URI(location));

            // todo: get values from config
            var config = new Configuration();
            config.set("fs.s3a.access.key", "admin");
            config.set("fs.s3a.secret.key", "password");
            config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            config.set("fs.s3a.path.style.access", "true");
            config.set("fs.s3a.endpoint", "http://localhost:9000");

            var fs = FileSystem.get(new URI(location), config);
            var out = fs.create(path);
            var writer = new DataFileWriter<GenericRecord>(new GenericDatumWriter<>(schema)).create(schema, out);

            logger.infof("Successfully opened writer for `%s`", location);

            return writer;
        } catch (URISyntaxException e) {
            // todo: domain URI error
            throw new RuntimeException(e);
        } catch (IOException e) {
            // todo: domain IO error
            logger.errorf(e, "Error happened while creating avro writer: %s", e.getLocalizedMessage());
            throw new RuntimeException(e);
        }
    }
}
