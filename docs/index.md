# debezium-postgres2lake

`debezium-postgres2lake` is a Java 21 / Quarkus multi-module CDC solution that processes PostgreSQL change events and persists them into data lake and lakehouse storage formats.

This project provides a complete, extensible foundation for CDC processing from source database events to analytics-ready lake targets.

## Purpose

Teams often need CDC pipelines they can inspect, adapt, and evolve. This project provides:

- A structured alternative to ad-hoc CDC scripts
- A modular codebase for format and integration customization
- A practical path from PoC to production-oriented CDC processing architecture

## End-to-end flow

1. PostgreSQL emits CDC via logical replication
2. Embedded Debezium engine captures change events
3. Events are normalized into internal records with operation metadata
4. Format-specific writers persist records to Avro/Parquet/ORC/Iceberg/Paimon targets
5. Optional SPI handlers integrate catalog sync, commit notifications, and readiness markers

## Core capabilities

- Multiple output formats: Avro, Parquet, ORC, Iceberg, Paimon
- Operation-aware CDC processing for insert, update, and delete events
- Schema evolution handling for table formats and schema-aware file writing
- Extension APIs and reference implementations for: data catalog integrations, commit event emission, readiness marker emission

## Continue reading

- [Requirements](requirements.md)
- [Getting Started](getting-started.md)
- [Features](features.md)
- [Schema evolution](schema-evolution.md)
- [Formats and Extensions](formats-and-extensions.md)
- [SPI extensions overview](spi/overview.md)
