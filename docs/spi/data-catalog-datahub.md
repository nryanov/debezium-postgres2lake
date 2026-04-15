# SPI: Data Catalog (DataHub)

DataHub integration emits dataset metadata and schema to a DataHub GMS endpoint.

Provider class:
`io.debezium.postgres2lake.extensions.data.catalog.datahub.DataHubDataCatalogProvider`

API module: `extensions/data-catalog-api`

## Configuration example

```properties
debezium.extensions.data-catalog.name=io.debezium.postgres2lake.extensions.data.catalog.datahub.DataHubDataCatalogProvider

debezium.extensions.data-catalog.properties.datahub.server=http://datahub-gms:8080
debezium.extensions.data-catalog.properties.datahub.token=replace-with-token
debezium.extensions.data-catalog.properties.datahub.platform=postgres
debezium.extensions.data-catalog.properties.datahub.fabric=PROD
```

## Available configs

| Property | Required | Description | Values / examples |
|---|---|---|---|
| `debezium.extensions.data-catalog.name` | Yes | SPI provider class name | `...DataHubDataCatalogProvider` |
| `debezium.extensions.data-catalog.properties.datahub.server` | Yes | DataHub GMS URL | `http://datahub-gms:8080` |
| `debezium.extensions.data-catalog.properties.datahub.token` | No | Bearer token | access token |
| `debezium.extensions.data-catalog.properties.datahub.platform` | No | DataHub platform name | default `postgres` |
| `debezium.extensions.data-catalog.properties.datahub.fabric` | No | DataHub fabric | `PROD`, `DEV` (default `PROD`) |
