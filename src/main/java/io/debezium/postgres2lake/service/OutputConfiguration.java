package io.debezium.postgres2lake.service;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;
import io.debezium.postgres2lake.infrastructure.format.avro.AvroCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.orc.OrcCompressionCodec;
import io.debezium.postgres2lake.infrastructure.format.parquet.ParquetCompressionCodec;
import io.smallrye.config.ConfigMapping;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "output")
public interface OutputConfiguration {
    OutputFormat format();

    Threshold threshold();

    Optional<Avro> avro();

    Optional<Parquet> parquet();

    Optional<Orc> orc();

    Optional<Iceberg> iceberg();

    Optional<Paimon> paimon();

    interface Avro {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<AvroCompressionCodec> codec();
    }

    interface Parquet {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<ParquetCompressionCodec> codec();
    }

    interface Orc {
        FileIO fileIO();

        OutputNamingStrategy namingStrategy();

        Optional<OrcCompressionCodec> codec();
    }

    interface Iceberg {
        String name();

        Map<String, String> properties();

        Optional<FileIO> fileIO();
    }

    interface Paimon {
        Map<String, String> properties();

        Optional<FileIO> fileIO();
    }

    interface FileIO {
        Map<String, String> properties();
    }

    interface OutputNamingStrategy {
        OutputPartitionStrategy partitioner();

        OutputFileNameGenerationStrategy fileName();
    }

    interface Threshold {
        int records();

        Duration time();
    }
}
