package io.debezium.postgres2lake.parquet.infrastructure.appender;

import io.debezium.postgres2lake.parquet.infrastructure.ParquetTableWriter;

public interface ParquetEventAppenderFactory {
    ParquetEventAppender create(ParquetTableWriter writer);
}
