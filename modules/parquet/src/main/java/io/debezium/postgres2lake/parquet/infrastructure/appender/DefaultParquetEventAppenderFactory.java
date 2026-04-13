package io.debezium.postgres2lake.parquet.infrastructure.appender;

import io.debezium.postgres2lake.parquet.infrastructure.ParquetTableWriter;

public class DefaultParquetEventAppenderFactory implements ParquetEventAppenderFactory {
    @Override
    public ParquetEventAppender create(ParquetTableWriter writer) {
        return new ParquetEventAppender(writer);
    }
}
