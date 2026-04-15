# Paimon format

`modules/paimon` writes CDC output to Apache Paimon tables.

## Configuration example

```properties
debezium.output.paimon.threshold.records=10
debezium.output.paimon.threshold.time=5m

# Paimon catalog properties (JDBC example)
debezium.output.paimon.properties.type=jdbc
debezium.output.paimon.properties.warehouse=s3a://warehouse/paimon-jdbc
debezium.output.paimon.properties.jdbc-url=jdbc:postgresql://postgres:5432/paimon_catalog
debezium.output.paimon.properties.jdbc-user=postgres
debezium.output.paimon.properties.jdbc-password=postgres
debezium.output.paimon.properties.jdbc-driver=org.postgresql.Driver
debezium.output.paimon.properties.jdbc-table-prefix=paimon_

debezium.output.paimon.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.paimon.file-io.properties.fs.s3a.access.key=admin
debezium.output.paimon.file-io.properties.fs.s3a.secret.key=password
debezium.output.paimon.file-io.properties.fs.s3a.path.style.access=true
debezium.output.paimon.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Paimon configs

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
| `debezium.output.paimon.threshold.records` | Yes | Flush threshold by record count | `10` |
| `debezium.output.paimon.threshold.time` | Yes | Flush threshold by elapsed time | `5m`, `30s` |
| `debezium.output.paimon.properties.*` | Yes | Paimon catalog/runtime properties map | `type=jdbc`, `warehouse=...` |
| `debezium.output.paimon.file-io.properties.*` | No | Hadoop/S3A file IO properties map | `fs.s3a.endpoint=...` |

## Common catalog patterns

- JDBC catalog:
  - `debezium.output.paimon.properties.type=jdbc`
  - `debezium.output.paimon.properties.jdbc-url=...`
  - `debezium.output.paimon.properties.jdbc-user=...`
  - `debezium.output.paimon.properties.jdbc-password=...`
- Hive catalog:
  - `debezium.output.paimon.properties.type=hive`
  - `debezium.output.paimon.properties.uri=thrift://hive-metastore:9083`
  - `debezium.output.paimon.properties.location-in-properties=true`
