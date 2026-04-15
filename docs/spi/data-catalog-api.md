# SPI API: Data Catalog

Root API module: `extensions/data-catalog-api`

This extension point allows publishing table schema metadata to external catalog systems.

## Configuration root

| Property pattern | Description |
|---|---|
| `debezium.extensions.data-catalog.name` | Provider class name |
| `debezium.extensions.data-catalog.properties.*` | Provider-specific properties map |

## Implementations

| Implementation module | Provider class | Docs |
|---|---|---|
| `extensions/data-catalog-openmetadata` | `io.debezium.postgres2lake.extensions.data.catalog.openmetadata.OpenMetadataDataCatalogProvider` | [OpenMetadata](data-catalog-openmetadata.md) |
| `extensions/data-catalog-datahub` | `io.debezium.postgres2lake.extensions.data.catalog.datahub.DataHubDataCatalogProvider` | [DataHub](data-catalog-datahub.md) |

If no provider class is set, the default no-op handler from `extensions/data-catalog-api` is used.
