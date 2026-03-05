package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.EventSaver;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.infrastructure.file.ProcessingTimeEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.file.UuidEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.partitioner.EventTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.ProcessedTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.UnpartitionedEventPartitioner;
import io.debezium.postgres2lake.infrastructure.s3.S3AvroEventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3IcebergEventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3OrcEventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3PaimonEventSaver;
import io.debezium.postgres2lake.infrastructure.s3.S3ParquetEventSaver;
import io.debezium.postgres2lake.service.OutputConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.inject.Produces;
import jakarta.inject.Inject;

@ApplicationScoped
public class ApplicationBeans {
    @Inject
    private OutputConfiguration outputConfiguration;

    @Produces
    EventSaver eventSaver() {
        return switch (outputConfiguration.format()) {
            case AVRO -> {
                if (outputConfiguration.avro().isEmpty()) {
                    throw new IllegalArgumentException("Empty avro format output configuration");
                }

                var avro = outputConfiguration.avro().get();
                var locationGenerator = resolveOutputLocationGenerator(avro.namingStrategy(), OutputFileFormat.avro);
                yield new S3AvroEventSaver(locationGenerator, avro.fileIO());
            }
            case ORC -> {
                if (outputConfiguration.orc().isEmpty()) {
                    throw new IllegalArgumentException("Empty orc format output configuration");
                }

                var orc = outputConfiguration.orc().get();
                var locationGenerator = resolveOutputLocationGenerator(orc.namingStrategy(), OutputFileFormat.orc);
                yield new S3OrcEventSaver(locationGenerator, orc.fileIO());
            }
            case PARQUET -> {
                if (outputConfiguration.parquet().isEmpty()) {
                    throw new IllegalArgumentException("Empty parquet format output configuration");
                }

                var parquet = outputConfiguration.parquet().get();
                var locationGenerator = resolveOutputLocationGenerator(parquet.namingStrategy(), OutputFileFormat.parquet);
                yield new S3ParquetEventSaver(locationGenerator, parquet.fileIO());
            }
            case ICEBERG -> {
                if (outputConfiguration.iceberg().isEmpty()) {
                    throw new IllegalArgumentException("Empty iceberg format output configuration");
                }

                yield new S3IcebergEventSaver();
            }
            case PAIMON -> {
                if (outputConfiguration.paimon().isEmpty()) {
                    throw new IllegalArgumentException("Empty paimon format output configuration");
                }

                yield new S3PaimonEventSaver();
            }
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
        };

        return new OutputLocationGenerator(partitionStrategy, namingStrategy, format);
    }
}
