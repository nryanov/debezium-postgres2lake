# debezium-postgres2lake

`debezium-postgres2lake` streams PostgreSQL CDC events through Debezium and writes them to lake-oriented storage formats.

It is a Java 21 and Quarkus-based multi-module project focused on practical local development and experimentation.

## What this project does

- Captures database changes from PostgreSQL (logical replication)
- Processes change events with Debezium
- Writes data in lake-friendly formats for analytics workloads
- Supports optional integrations through extension modules

## Core capabilities

- Multiple output formats: Avro, Parquet, ORC, Iceberg, Paimon
- Extension APIs and implementations for:
  - Data catalog integrations
  - Commit event emission
  - Readiness marker emission
- Local examples using Docker Compose with PostgreSQL, MinIO, and supporting services
