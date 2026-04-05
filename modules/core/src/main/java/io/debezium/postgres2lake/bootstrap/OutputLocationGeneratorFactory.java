package io.debezium.postgres2lake.bootstrap;

import io.debezium.postgres2lake.domain.model.OutputFileFormat;
import io.debezium.postgres2lake.infrastructure.file.ProcessingTimeEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.file.UuidEventFileNameGenerator;
import io.debezium.postgres2lake.infrastructure.partitioner.EventTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.ProcessedTimeEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.RecordFieldEventPartitioner;
import io.debezium.postgres2lake.infrastructure.partitioner.UnpartitionedEventPartitioner;
import io.debezium.postgres2lake.config.CommonConfiguration;
import io.debezium.postgres2lake.service.OutputLocationGenerator;

public final class OutputLocationGeneratorFactory {

    private OutputLocationGeneratorFactory() {
    }

    public static OutputLocationGenerator resolve(CommonConfiguration.OutputNamingStrategy strategy, OutputFileFormat format) {
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
