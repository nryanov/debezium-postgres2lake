# Paimon format

`modules/paimon` writes CDC output to Apache Paimon tables.

## Configuration example

```properties
debezium.output.paimon.threshold.records=10
debezium.output.paimon.threshold.time=5m

# Paimon catalog properties (JDBC example)
debezium.output.paimon.properties.metastore=jdbc
debezium.output.paimon.properties.warehouse=s3a://warehouse/paimon-jdbc
debezium.output.paimon.properties.uri=jdbc:postgresql://postgres:5432/paimon_catalog?user=postgres&password=postgres

# Optional default properties for every created Paimon table
debezium.output.paimon.default-table-properties.bucket=4
debezium.output.paimon.default-table-properties.changelog-producer=input

# Optional per-table overrides (key must match runtime table identifier)
debezium.output.paimon.table-specs.default_public.demo_orders.properties.bucket=8
debezium.output.paimon.table-specs.default_public.demo_orders.properties.file.format=parquet
debezium.output.paimon.table-specs.default_public.demo_orders.properties.changelog-producer=lookup

debezium.output.paimon.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.paimon.file-io.properties.fs.s3a.access.key=admin
debezium.output.paimon.file-io.properties.fs.s3a.secret.key=password
debezium.output.paimon.file-io.properties.fs.s3a.path.style.access=true
debezium.output.paimon.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Paimon configs

| Property                                                         | Required | Description                                             | Values / examples                                           |
|------------------------------------------------------------------|----------|---------------------------------------------------------|-------------------------------------------------------------|
| `debezium.output.paimon.threshold.records`                       | Yes      | Flush threshold by record count                         | `10`                                                        |
| `debezium.output.paimon.threshold.time`                          | Yes      | Flush threshold by elapsed time                         | `5m`, `30s`                                                 |
| `debezium.output.paimon.properties.*`                            | Yes      | Paimon catalog/runtime properties map                   | `metastore=jdbc`, `warehouse=...`, `uri=...`               |
| `debezium.output.paimon.file-io.properties.*`                    | No       | Hadoop/S3A file IO properties map                       | `debezium.output.paimon.file-io.properties.fs.s3a.endpoint=...` |
| `debezium.output.paimon.default-table-properties.*`              | No       | Properties applied to every created table               | `bucket=4`                                                  |
| `debezium.output.paimon.table-specs.<tableIdentifier>.properties.*` | No    | Per-table properties overriding default table properties | `bucket=8`, `file.format=parquet`                          |

## Custom table properties

`default-table-properties.*` are applied first to each created table, then `table-specs.<tableIdentifier>.properties.*` values override them for matching tables.

Use a table identifier key that matches the runtime full table name (for example `default_public.demo_orders`):

```properties
debezium.output.paimon.default-table-properties.bucket=4
debezium.output.paimon.default-table-properties.file.format=parquet
debezium.output.paimon.default-table-properties.changelog-producer=input

debezium.output.paimon.table-specs.default_public.demo_orders.properties.bucket=8
debezium.output.paimon.table-specs.default_public.demo_orders.properties.changelog-producer=lookup
```

## Common catalog patterns

- JDBC catalog:
  - `debezium.output.paimon.properties.metastore=jdbc`
  - `debezium.output.paimon.properties.uri=jdbc:postgresql://...`
- Hive catalog:
  - `debezium.output.paimon.properties.metastore=hive`
  - `debezium.output.paimon.properties.uri=thrift://hive-metastore:9083`
  - `debezium.output.paimon.properties.location-in-properties=true`
