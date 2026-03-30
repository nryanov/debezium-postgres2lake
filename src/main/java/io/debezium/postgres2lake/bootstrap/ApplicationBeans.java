package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.infrastructure.debezium.DebeziumConfiguration;
import io.debezium.postgres2lake.infrastructure.debezium.avro.GenericRecordBinaryDeserializer;
import io.debezium.postgres2lake.infrastructure.debezium.avro.GenericRecordConfluentDeserializer;
import io.debezium.postgres2lake.infrastructure.debezium.avro.UnwrappedEventRecordDeserializer;
import io.debezium.postgres2lake.infrastructure.file.ProcessingTimeEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.file.UuidEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroCompressionCodec;
import io.debezium.postgres2lake.infrastructure.partitioner.EventTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.ProcessedTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.RecordFieldEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.UnpartitionedEventPartitioner;
import io.debezium.postgres2lake.infrastructure.s3.S3AvroEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;
import jakarta.inject.Singleton;

@ApplicationScoped
public class ApplicationBeans {
    @Inject
    private OutputConfiguration outputConfiguration;

    @Inject
    private DebeziumConfiguration debeziumConfiguration;

    @Singleton
    @Produces
    UnwrappedEventRecordDeserializer unwrappedEventRecordDeserializer() {
        var avro = debeziumConfiguration.avro();

        var genericDeserializer = switch (avro.format()) {
            case CONFLUENT -> new GenericRecordConfluentDeserializer(avro.properties());
            case BINARY -> new GenericRecordBinaryDeserializer();
        };

        return new UnwrappedEventRecordDeserializer(genericDeserializer);
    }

    @Singleton
    @Produces
    EventSaver eventSaver() {
        return switch (outputConfiguration.format()) {
            case AVRO -> {
                if (outputConfiguration.avro().isEmpty()) {
                    throw new IllegalArgumentException("Empty avro format output configuration");
                }

                var avro = outputConfiguration.avro().get();
                var locationGenerator = resolveOutputLocationGenerator(avro.namingStrategy(), OutputFileFormat.avro);
                yield new S3AvroEventSaver(
                        outputConfiguration.threshold(),
                        locationGenerator,
                        avro.fileIO(),
                        avro.codec().orElse(AvroCompressionCodec.NONE)
                );
            }
            default -> throw new IllegalArgumentException("expected only avro");
        };
    }

    private OutputLocationGenerator resolveOutputLocationGenerator(OutputConfiguration.OutputNamingStrategy strategy, OutputFileFormat format) {
        var namingStrategy = switch (strategy.fileName()) {
            case UUID -> new UuidEventFileNameGenerator();
            case PROCESSING_TIME -> new ProcessingTimeEventFileNameGenerator();
        };

        var partitionStrategy = switch (strategy.partitioner()) {
            case UNPARTITIONED -> new UnpartitionedEventPartitioner();
            case PROCESSING_TIME -> new ProcessedTimeEventPartitioner();
            case EVENT_TIME -> new EventTimeEventPartitioner();
            case RECORD_FIELD -> {
                var field = strategy.recordPartitionField()
                        .filter(s -> !s.isBlank())
                        .orElseThrow(() -> new IllegalArgumentException(
                                "output.*.naming-strategy.record-partition-field is required when partitioner is RECORD_FIELD"));
                yield new RecordFieldEventPartitioner(field);
            }
        };

        return new OutputLocationGenerator(partitionStrategy, namingStrategy, format);
    }
}
