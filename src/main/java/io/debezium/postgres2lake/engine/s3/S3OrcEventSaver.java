package io.debezium.postgres2lake.engine.s3;

import io.debezium.postgres2lake.engine.EventCommitter;
import io.debezium.postgres2lake.engine.EventRecord;
import jakarta.enterprise.context.ApplicationScoped;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.orc.CompressionKind;
import org.apache.orc.OrcFile;
import org.apache.orc.TypeDescription;
import org.apache.orc.Writer;
import org.jboss.logging.Logger;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

@ApplicationScoped
public class S3OrcEventSaver implements EventSaver {
    private record OpenedWriter(Writer writer, VectorizedRowBatch batch) {}

    private static final Logger logger = Logger.getLogger(S3OrcEventSaver.class);

    private final List<EventCommitter> committers;
    private final Map<String, OpenedWriter> openedDescriptors;

    private final Duration timeoutThreshold;
    private final int totalRecordsThreshold;

    private final ScheduledExecutorService scheduledExecutor;

    private int currentRecords;

    public S3OrcEventSaver() {
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

    @SuppressWarnings({"unchecked", "resource"})
    private void backlogData(Stream<EventRecord> events, EventCommitter committer) {
        synchronized (this) {
            logger.info("Append records (stream)");
            events.forEach(event -> {
                var destination = event.destination();
                var location = generateLocation("warehouse", event.destination());
                var currentEvents = openedDescriptors.computeIfAbsent(destination, ignored -> {
                    var writer = createFileWriter(location, fromAvro(event.flattenSchema()));
                    var batch = writer.getSchema().createRowBatch(); // todo: configure batch size

                    return new OpenedWriter(writer, batch);
                });

                try {
                    var batch = currentEvents.batch;
                    var row = batch.size;
                    batch.size += 1;

                    // todo: use SMT to unwrap debezium data
                    var value = (GenericRecord) event.value().get("after");
                    var i = 0;
                    // todo: map types correctly. Currently only long is supported
                    for (var field : value.getSchema().getFields()) {
                        var fieldValue = value.get(field.name());
                        var vector = batch.cols[i++];
                        var longVector = (LongColumnVector) vector;
                        longVector.isNull[row] = false;
                        longVector.vector[row] = (Long) fieldValue;
                    }

                    currentRecords++;

                    if (batch.size == batch.getMaxSize()) {
                        currentEvents.writer.addRowBatch(batch);
                        batch.reset();
                    }
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
                    var openedFile = entry.getValue();

                    if (openedFile.batch.size != 0) {
                        logger.infof("Add rows batch: %s", openedFile.batch.count());
                        openedFile.writer.addRowBatch(openedFile.batch);
                        openedFile.batch.reset();
                    }

                    openedFile.writer.close();
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

    private TypeDescription fromAvro(Schema schema) {
        logger.infof("Schema: %s", schema.toString());
        var orcSchema = switch (schema.getType()) {
            case INT -> TypeDescription.createInt();
            case STRING -> TypeDescription.createString();
            case BOOLEAN -> TypeDescription.createBoolean();
            case LONG -> TypeDescription.createLong();
            case FLOAT -> TypeDescription.createFloat();
            case DOUBLE -> TypeDescription.createDouble();
            case BYTES -> null;
            case MAP -> null;
            case ENUM -> null;
            case FIXED -> null;
            case UNION -> null;
            case ARRAY -> null;
            case RECORD -> null;
            case null, default -> throw new IllegalArgumentException("Unsupported type");
        };

        logger.infof("ORC schema: %s", orcSchema);
//        return orcSchema;
        return TypeDescription.createStruct().addField("field1", TypeDescription.createLong());
    }

    private Writer createFileWriter(String location, TypeDescription schema) {
        try {
            // todo: get values from config
            var config = new Configuration();
            config.set("fs.s3a.access.key", "admin");
            config.set("fs.s3a.secret.key", "password");
            config.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
            config.set("fs.s3a.path.style.access", "true");
            config.set("fs.s3a.endpoint", "http://localhost:9000");

            var options = OrcFile.writerOptions(config)
                    .setSchema(schema)
//                    .stripeSize(64 * 1024 * 1024) // 64 Mb
                    .useUTCTimestamp(true)
                    .compress(CompressionKind.NONE)
                    .callback(new OrcFile.WriterCallback() {
                        @Override
                        public void preStripeWrite(OrcFile.WriterContext context) throws IOException {
                            logger.infof("[PRE-STRIPE-WRITE statistics] rows: %s, memory: %s", context.getWriter().getNumberOfRows(), context.getWriter().estimateMemory());
                        }

                        @Override
                        public void preFooterWrite(OrcFile.WriterContext context) throws IOException {
                            logger.infof("Footer write");
                        }
                    });


            var writer = OrcFile.createWriter(new Path(location), options);
            return writer;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String generateLocation(String bucket, String destination) {
        return String.format(
                "s3a://%s/%s/%s_%s.orc",
                bucket,
                destination, // todo: -> schema/table/[file1, file2, .., fileN]
                destination,
                System.currentTimeMillis()
        );
    }
}
