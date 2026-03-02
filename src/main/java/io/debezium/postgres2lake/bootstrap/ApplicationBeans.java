package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.infrastructure.file.ProcessingTimeEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.file.UuidEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.partitioner.EventTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.ProcessedTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.UnpartitionedEventPartitioner;
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
    OutputLocationGenerator outputLocationGenerator() {
        return switch (outputConfiguration.format()) {
            case AVRO -> {
                if (outputConfiguration.avro().isEmpty()) {
                    throw new IllegalArgumentException("Empty avro format output configuration");
                }

                var avro = outputConfiguration.avro().get();
                yield resolveOutputLocationGenerator(avro.namingStrategy(), OutputFileFormat.avro);
            }
            case ORC -> {
                if (outputConfiguration.orc().isEmpty()) {
                    throw new IllegalArgumentException("Empty orc format output configuration");
                }

                var orc = outputConfiguration.orc().get();
                yield resolveOutputLocationGenerator(orc.namingStrategy(), OutputFileFormat.orc);
            }
            case PARQUET -> {
                if (outputConfiguration.parquet().isEmpty()) {
                    throw new IllegalArgumentException("Empty parquet format output configuration");
                }

                var parquet = outputConfiguration.parquet().get();
                yield resolveOutputLocationGenerator(parquet.namingStrategy(), OutputFileFormat.parquet);
            }
            case null, default -> null;
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
