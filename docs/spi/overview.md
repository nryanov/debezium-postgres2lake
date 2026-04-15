# SPI extensions overview

SPI extensions let you plug optional behaviors into the processing pipeline.

## Global SPI key pattern

| Key pattern | Description |
|---|---|
| `debezium.extensions.<extension>.name` | Provider class name to load via `ServiceLoader` |
| `debezium.extensions.<extension>.properties.*` | Provider-specific key-value map passed to `initialize(...)` |

If `name` is omitted, the default no-op handler is used.

## Grouped by API module

| API module | Purpose | API docs | Implementations |
|---|---|---|
| `extensions/data-catalog-api` | Register/update schema metadata in external catalogs | [Data Catalog API](data-catalog-api.md) | [OpenMetadata](data-catalog-openmetadata.md), [DataHub](data-catalog-datahub.md) |
| `extensions/commit-event-emitter-api` | Emit file commit notifications | [Commit Event API](commit-event-api.md) | [Kafka](commit-event-emitter-kafka.md) |
| `extensions/readiness-marker-event-emitter-api` | Write readiness markers after commit cycles | [Readiness Marker API](readiness-marker-api.md) | [S3](readiness-marker-s3.md) |
