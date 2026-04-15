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
debezium.output.iceberg.table-specs.demo_orders.partition-by=lake_part
debezium.output.iceberg.table-specs.demo_orders.sort-by=id
debezium.output.iceberg.table-specs.demo_orders.location=s3a://warehouse/iceberg-jdbc/demo_orders
debezium.output.iceberg.table-specs.demo_orders.properties.format-version=2

debezium.output.iceberg.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.iceberg.file-io.properties.fs.s3a.access.key=admin
debezium.output.iceberg.file-io.properties.fs.s3a.secret.key=password
debezium.output.iceberg.file-io.properties.fs.s3a.path.style.access=true
debezium.output.iceberg.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Iceberg configs

| Property | Required | Description | Values / examples |
|---|---|---|---|
| `quarkus.http.port` | No | HTTP/health port for app container | `9090` |
| `debezium.engine.name` | Yes | Debezium engine name | `examples` |
| `debezium.engine.database.hostname` | Yes | PostgreSQL host | `postgres` |
| `debezium.engine.database.dbname` | Yes | PostgreSQL database name | `postgres` |
| `debezium.engine.database.port` | Yes | PostgreSQL port | `5432` |
| `debezium.engine.database.user` | Yes | PostgreSQL user | `postgres` |
| `debezium.engine.database.password` | Yes | PostgreSQL password | `postgres` |
| `debezium.engine.publication.name` | Yes | Logical replication publication | `debezium` |
| `debezium.engine.slot.name` | Yes | Logical replication slot | `debezium` |
| `debezium.engine.plugin.name` | Yes | Debezium Postgres plugin | `pgoutput` |
| `debezium.engine.snapshot.mode` | Yes | Initial snapshot behavior | `NO_DATA` |
| `debezium.engine.topic.prefix` | Yes | Kafka Connect topic prefix | `default` |
| `debezium.engine.offset.storage` | Yes | Offset backing store class | `org.apache.kafka.connect.storage.MemoryOffsetBackingStore` |
| `debezium.avro.format` | Yes | Avro serialization mode for Debezium payload conversion | `CONFLUENT`, `BINARY` |
| `debezium.avro.properties.*` | No | Avro serializer properties map | `schema.registry.url=...` |
| `debezium.output.iceberg.threshold.records` | Yes | Flush threshold by record count | `10` |
| `debezium.output.iceberg.threshold.time` | Yes | Flush threshold by elapsed time | `5m`, `30s` |
| `debezium.output.iceberg.name` | Yes | Logical Iceberg writer name | `examples` |
| `debezium.output.iceberg.properties.*` | Yes | Catalog and Iceberg runtime properties map | `type=jdbc`, `warehouse=...` |
| `debezium.output.iceberg.file-io.properties.*` | No | Hadoop/S3A file IO properties map | `fs.s3a.endpoint=...` |
| `debezium.output.iceberg.table-specs.<table>.location` | No | Per-table location override | `s3a://warehouse/iceberg-jdbc/demo_orders` |
| `debezium.output.iceberg.table-specs.<table>.properties.*` | No | Per-table Iceberg table properties | `format-version=2` |
| `debezium.output.iceberg.table-specs.<table>.partition-by` | No | Per-table partition columns list | `lake_part` |
| `debezium.output.iceberg.table-specs.<table>.sort-by` | No | Per-table sort columns list | `id` |

## Common catalog patterns

- JDBC catalog: `type=jdbc` with JDBC connection properties
- Nessie catalog: `type=nessie`, `uri=http://nessie:19120/api/v1`, `ref=main`
- Hive catalog: `type=hive`, `uri=thrift://hive-metastore:9083`
