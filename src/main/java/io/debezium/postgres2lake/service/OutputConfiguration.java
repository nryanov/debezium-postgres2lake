package io.debezium.postgres2lake.service;

import io.debezium.postgres2lake.domain.model.OutputFileNameGenerationStrategy;
import io.debezium.postgres2lake.domain.model.OutputFormat;
import io.debezium.postgres2lake.domain.model.OutputPartitionStrategy;
import io.smallrye.config.ConfigMapping;

import java.util.Map;
import java.util.Optional;

@ConfigMapping(prefix = "output")
public interface OutputConfiguration {
    OutputFormat format();

    Optional<Avro> avro();

    Optional<Parquet> parquet();

    Optional<Orc> orc();

    Optional<Iceberg> iceberg();

    Optional<Paimon> paimon();

    interface Avro {
        S3 s3();

        OutputNamingStrategy namingStrategy();
    }

    interface Parquet {
        S3 s3();

        OutputNamingStrategy namingStrategy();
    }

    interface Orc {
        S3 s3();

        OutputNamingStrategy namingStrategy();
    }

    interface Iceberg {
        Map<String, String> properties();
    }

    interface Paimon {
        Map<String, String> properties();
    }

    interface S3 {
        String accessKey();

        String secretAccessKey();

        String endpoint();

        Map<String, String> overrides();
    }

    interface OutputNamingStrategy {
        OutputPartitionStrategy partitioner();

        OutputFileNameGenerationStrategy fileName();
    }
}
