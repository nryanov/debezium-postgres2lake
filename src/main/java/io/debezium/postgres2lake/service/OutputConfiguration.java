package io.debezium.postgres2lake.service;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroCompressionCodec;
import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "output")
public interface OutputConfiguration {
    OutputFormat format();

    Threshold threshold();

    Optional<Avro> avro();

    interface Avro {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<AvroCompressionCodec> codec();
    }

    interface FileIO {
        Map<String, String> properties();
    }

    interface OutputNamingStrategy {
        OutputPartitionStrategy partitioner();

        OutputFileNameGenerationStrategy fileName();

        Optional<String> recordPartitionField();
    }

    interface Threshold {
        int records();

        Duration time();
    }
}
