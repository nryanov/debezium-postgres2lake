package io.debezium.postgres2lake.infrastructure.file;

import io.debezium.postgres2lake.domain.EventFileNameGenerator;
import io.debezium.postgres2lake.domain.model.OutputFileFormat;

public class ProcessingTimeEventFileNameGenerator implements EventFileNameGenerator {
    @Override
    public String generate(String prefix, OutputFileFormat format) {
        var name = System.currentTimeMillis();
        return String.format("%s/%s.%s", prefix, name, format.name());
    }
}
