# Iceberg format

`modules/iceberg` writes CDC output to Apache Iceberg tables.

## Configuration example

```properties
debezium.output.iceberg.threshold.records=10
debezium.output.iceberg.threshold.time=5m
debezium.output.iceberg.name=examples

# Iceberg catalog properties (JDBC example)
debezium.output.iceberg.properties.type=jdbc
debezium.output.iceberg.properties.uri=jdbc:postgresql://postgres:5432/iceberg_catalog
debezium.output.iceberg.properties.jdbc.user=postgres
debezium.output.iceberg.properties.jdbc.password=postgres
debezium.output.iceberg.properties.warehouse=s3a://warehouse/iceberg-jdbc
debezium.output.iceberg.properties.io-impl=org.apache.iceberg.aws.s3.S3FileIO
debezium.output.iceberg.properties.s3.endpoint=http://minio:9000
debezium.output.iceberg.properties.s3.access-key-id=admin
debezium.output.iceberg.properties.s3.secret-access-key=password
debezium.output.iceberg.properties.s3.path-style-access=true

# Optional per-table spec
debezium.output.iceberg.table-specs.default_public.demo_orders.partition-by=lake_part
debezium.output.iceberg.table-specs.default_public.demo_orders.sort-by=id
debezium.output.iceberg.table-specs.default_public.demo_orders.location=s3a://warehouse/iceberg-jdbc/demo_orders
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.format-version=2
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.write.target-file-size-bytes=134217728

debezium.output.iceberg.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.iceberg.file-io.properties.fs.s3a.access.key=admin
debezium.output.iceberg.file-io.properties.fs.s3a.secret.key=password
debezium.output.iceberg.file-io.properties.fs.s3a.path.style.access=true
debezium.output.iceberg.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Iceberg configs

| Property                                                             | Required | Description                                             | Values / examples                                           |
|----------------------------------------------------------------------|----------|---------------------------------------------------------|-------------------------------------------------------------|
| `debezium.output.iceberg.threshold.records`                          | Yes      | Flush threshold by record count                         | `10`                                                        |
| `debezium.output.iceberg.threshold.time`                             | Yes      | Flush threshold by elapsed time                         | `5m`, `30s`                                                 |
| `debezium.output.iceberg.name`                                       | Yes      | Logical Iceberg writer name                             | `examples`                                                  |
| `debezium.output.iceberg.properties.*`                               | Yes      | Catalog and Iceberg runtime properties map              | `type=jdbc`, `warehouse=...`                                |
| `debezium.output.iceberg.file-io.properties.*`                       | No       | Hadoop/S3A file IO properties map                       | `debezium.output.iceberg.file-io.properties.fs.s3a.endpoint=...` |
| `debezium.output.iceberg.table-specs.<tableIdentifier>.location`     | No       | Per-table location override                             | `s3a://warehouse/iceberg-jdbc/demo_orders`                  |
| `debezium.output.iceberg.table-specs.<tableIdentifier>.properties.*` | No       | Per-table Iceberg table properties                      | `format-version=2`, `write.target-file-size-bytes=134217728` |
| `debezium.output.iceberg.table-specs.<tableIdentifier>.partition-by` | No       | Per-table partition columns list                        | `lake_part`                                                 |
| `debezium.output.iceberg.table-specs.<tableIdentifier>.sort-by`      | No       | Per-table sort columns list                             | `id`                                                        |

## Custom table properties

Use `table-specs.<tableIdentifier>` keys for table-level overrides. The `<tableIdentifier>` value must match the runtime Iceberg table identifier string (for example `default_public.demo_orders`).

```properties
debezium.output.iceberg.table-specs.default_public.demo_orders.location=s3a://warehouse/iceberg-jdbc/demo_orders
debezium.output.iceberg.table-specs.default_public.demo_orders.partition-by=lake_part
debezium.output.iceberg.table-specs.default_public.demo_orders.sort-by=id
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.format-version=2
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.write.target-file-size-bytes=134217728
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.delete.mode=merge-on-read
debezium.output.iceberg.table-specs.default_public.demo_orders.properties.update.mode=merge-on-read
```

## Common catalog patterns

- JDBC catalog: `type=jdbc` with JDBC connection properties
- Nessie catalog: `type=nessie`, `uri=http://nessie:19120/api/v1`, `ref=main`
- Hive catalog: `type=hive`, `uri=thrift://hive-metastore:9083`
