package io.debezium.postgres2lake.infrastructure.file;

import io.debezium.postgres2lake.domain.EventFileNameGenerator;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;

import java.util.UUID;

public class UuidEventFileNameGenerator implements EventFileNameGenerator {
    @Override
    public String generate(String prefix, OutputFileFormat format) {
        var name = UUID.randomUUID().toString();
        return String.format("%s/%s.%s", prefix, name, format.name());
    }
}
