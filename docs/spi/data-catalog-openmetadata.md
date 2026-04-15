# SPI: Data Catalog (OpenMetadata)

OpenMetadata integration publishes table metadata and schema updates to an OpenMetadata server.

Provider class:
`io.debezium.postgres2lake.extensions.data.catalog.openmetadata.OpenMetadataDataCatalogProvider`

API module: `extensions/data-catalog-api`

## Configuration example

```properties
debezium.extensions.data-catalog.name=io.debezium.postgres2lake.extensions.data.catalog.openmetadata.OpenMetadataDataCatalogProvider

debezium.extensions.data-catalog.properties.openmetadata.host=http://openmetadata:8585/api
debezium.extensions.data-catalog.properties.openmetadata.jwt=replace-with-bot-jwt
debezium.extensions.data-catalog.properties.openmetadata.databaseSchema.fqn=service.database.schema
debezium.extensions.data-catalog.properties.openmetadata.validate.version=false
```

## Available configs

| Property | Required | Description | Values / examples |
|---|---|---|---|
| `debezium.extensions.data-catalog.name` | Yes | SPI provider class name | `...OpenMetadataDataCatalogProvider` |
| `debezium.extensions.data-catalog.properties.openmetadata.host` | Yes | OpenMetadata API base URL | `http://openmetadata:8585/api` |
| `debezium.extensions.data-catalog.properties.openmetadata.jwt` | Yes | JWT token | bot/service JWT |
| `debezium.extensions.data-catalog.properties.openmetadata.databaseSchema.fqn` | Yes | Target database schema FQN in OpenMetadata | `service.database.schema` |
| `debezium.extensions.data-catalog.properties.openmetadata.validate.version` | No | Validate OpenMetadata client/server version compatibility | `true`, `false` (default `false`) |
