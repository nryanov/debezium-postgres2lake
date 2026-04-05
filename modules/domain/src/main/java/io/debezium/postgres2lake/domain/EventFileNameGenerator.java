package io.debezium.postgres2lake.domain;

import io.debezium.postgres2lake.domain.model.OutputFileFormat;

public interface EventFileNameGenerator {
    String generate(String prefix, OutputFileFormat format);
}
