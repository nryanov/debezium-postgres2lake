# Schema Evolution

This page explains how schema changes are handled in `debezium-postgres2lake`.

## How schema evolution is detected

The pipeline compares consecutive Avro value schemas as events are processed.  
When a schema change is detected, the current batch is committed, schema change handling is applied, and a new appender lifecycle starts for the updated schema.

## Supported schema changes

The schema diff resolver supports the following evolution patterns:

- Add column
- Delete column
- Make field optional (required -> optional)
- Safe primitive widening (for example: `int` -> `long`, `float` -> `double`)
- Decimal widening with increased precision/scale where compatible

## Unsupported or incompatible changes

The pipeline fails fast for incompatible schema transitions, including:

- Making an optional field required
- Primitive-to-non-primitive (or incompatible structural) type changes
- Other unsupported type promotions

Fail-fast behavior prevents silent data corruption and makes incompatible changes explicit during processing.

## Behavior by sink type

Table formats:

- Iceberg and Paimon apply schema evolution to table metadata/DDL using the resolved schema diff
- After evolution is applied, writing continues with the updated schema

Object/file formats:

- Avro, Parquet, and ORC do not perform table DDL evolution in this codebase
- A new writer/appender is created for the new schema, producing new files with updated schema metadata

## Operational guidance

- Prefer additive schema changes in source tables
- Roll out incompatible DDL carefully and validate in a staging environment first
- Keep downstream consumers aware of schema version changes and compatibility constraints

## Related docs

- [Features](features.md)
- [Formats](formats.md)
- [SPI extensions overview](spi/overview.md)
