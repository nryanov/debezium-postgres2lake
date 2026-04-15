# Formats and Extensions

This repository is organized as a Gradle multi-module project.

## Format modules

- `modules/avro` - Avro output ([docs](formats/avro.md))
- `modules/parquet` - Parquet output ([docs](formats/parquet.md))
- `modules/orc` - ORC output ([docs](formats/orc.md))
- `modules/iceberg` - Iceberg table support ([docs](formats/iceberg.md))
- `modules/paimon` - Paimon table support ([docs](formats/paimon.md))

## Core and supporting modules

- `modules/core` - shared CDC processing logic
- `modules/domain` - shared domain model
- `modules/platform` - platform and dependency alignment
- `modules/jib` - container image resources/scripts

## Extension modules

### Data catalog

- `extensions/data-catalog-api`
- `extensions/data-catalog-openmetadata` ([docs](spi/data-catalog-openmetadata.md))
- `extensions/data-catalog-datahub` ([docs](spi/data-catalog-datahub.md))

### Commit event emitters

- `extensions/commit-event-emitter-api`
- `extensions/commit-event-emitter-kafka` ([docs](spi/commit-event-emitter-kafka.md))

### Readiness marker emitters

- `extensions/readiness-marker-event-emitter-api`
- `extensions/readiness-marker-event-emitter-s3` ([docs](spi/readiness-marker-s3.md))

## Test fixtures

The `modules/test-fixtures/` subtree provides reusable infrastructure for integration testing (for example PostgreSQL, S3, schema registry, and Nessie-related fixtures).
