# Features

`debezium-postgres2lake` is a CDC processing foundation from PostgreSQL events to lake/lakehouse targets.

## CDC processing model

- Source: PostgreSQL logical replication with Debezium PostgreSQL connector
- Engine: embedded Debezium engine inside the application
- Event normalization: change events are mapped to internal records with operation and metadata fields
- Sink model: format-specific appenders and savers write batched output to target storage/catalogs

## Supported CDC operations

The processing pipeline supports three operation types in the domain model:

- `INSERT`
- `UPDATE`
- `DELETE`

Debezium operation mapping:

- `c` (create) -> `INSERT`
- `r` (snapshot read) -> `INSERT`
- `u` (update) -> `UPDATE`
- `d` (delete) -> `DELETE`

## Output formats

Supported output modules:

- Avro
- Parquet
- ORC
- Iceberg
- Paimon

## Update/delete behavior by sink type

Table formats:

- Iceberg: operation-aware write semantics with equality-delete handling for update/delete flows
- Paimon: operation-aware changelog row kinds for insert/update/delete

Object/file formats:

- Avro, Parquet, ORC write schema-aware records through format appenders
- CDC operation metadata can be preserved in the record payload schema and used downstream

## Extension features (SPI)

Optional extension points are available for:

- Data catalog synchronization
- Commit event emission
- Readiness marker emission

Current implementations in this repository are reference integrations. You can extend them or provide your own implementations through SPI modules.

See:

- [SPI extensions overview](spi/overview.md)
- [Implementing SPI extensions](spi/implementing-extensions.md)
- [Schema evolution](schema-evolution.md)
