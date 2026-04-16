# SPI extensions overview

SPI extensions let you plug optional behaviors into the processing pipeline.

If you want to build your own extension module, see [Implementing SPI extensions](implementing-extensions.md).

## Global SPI key pattern

| Key pattern                                    | Description                                                 |
|------------------------------------------------|-------------------------------------------------------------|
| `debezium.extensions.<extension>.name`         | Provider class name to load via `ServiceLoader`             |
| `debezium.extensions.<extension>.properties.*` | Provider-specific key-value map passed to `initialize(...)` |

If `name` is omitted, the default no-op handler is used.

## Runtime extension JARs in Docker

Container images support loading SPI providers from a runtime directory without rebuilding the app image.

- Set `SPI_EXT_DIR` to a directory inside the container (for example `/work/spi`)
- Mount or copy extension JARs into that directory
- Keep using `debezium.extensions.<extension>.name=<provider.FQCN>` in application config

When `SPI_EXT_DIR` points to an existing directory, the container entrypoint starts Quarkus in classpath mode and prepends `${SPI_EXT_DIR}/*` before the application libraries so `ServiceLoader` can discover provider registrations.

## Grouped by API module

| API module                                      | Purpose                                              | API docs                                        | Implementations                                                                  |
|-------------------------------------------------|------------------------------------------------------|-------------------------------------------------|----------------------------------------------------------------------------------|
| `extensions/data-catalog-api`                   | Register/update schema metadata in external catalogs | [Data Catalog API](data-catalog-api.md)         | [OpenMetadata](data-catalog-openmetadata.md), [DataHub](data-catalog-datahub.md) |
| `extensions/commit-event-emitter-api`           | Emit file commit notifications                       | [Commit Event API](commit-event-api.md)         | [Kafka](commit-event-emitter-kafka.md)                                           |
| `extensions/readiness-marker-event-emitter-api` | Write readiness markers after commit cycles          | [Readiness Marker API](readiness-marker-api.md) | [S3](readiness-marker-s3.md)                                                     |
