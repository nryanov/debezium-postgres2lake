# Avro format

`modules/avro` writes CDC output files in Avro format.

## Configuration example

```properties
# Engine (shared)
debezium.engine.name=examples
debezium.engine.database.hostname=postgres
debezium.engine.database.dbname=postgres
debezium.engine.database.port=5432
debezium.engine.database.user=postgres
debezium.engine.database.password=postgres
debezium.engine.topic.prefix=default

# Avro serialization mode
debezium.avro.format=CONFLUENT
debezium.avro.properties.schema.registry.url=http://schema-registry:8080/apis/ccompat/v7

# Avro output
debezium.output.avro.threshold.records=10
debezium.output.avro.threshold.time=5m
debezium.output.avro.naming-strategy.partitioner=UNPARTITIONED
debezium.output.avro.naming-strategy.file-name=PROCESSING_TIME
debezium.output.avro.naming-strategy.storage=S3
debezium.output.avro.naming-strategy.target-path=warehouse
debezium.output.avro.codec=SNAPPY

debezium.output.avro.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.avro.file-io.properties.fs.s3a.access.key=admin
debezium.output.avro.file-io.properties.fs.s3a.secret.key=password
debezium.output.avro.file-io.properties.fs.s3a.path.style.access=true
debezium.output.avro.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Avro configs

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
| `debezium.avro.format` | Yes | Avro serialization mode | `CONFLUENT`, `BINARY` |
| `debezium.avro.properties.*` | No | Avro serializer properties map | `schema.registry.url=...` |
| `debezium.output.avro.threshold.records` | Yes | Flush threshold by record count | `10` |
| `debezium.output.avro.threshold.time` | Yes | Flush threshold by elapsed time | `5m`, `30s` |
| `debezium.output.avro.file-io.properties.*` | No | Hadoop/S3A file IO properties map | `fs.s3a.endpoint=...` |
| `debezium.output.avro.naming-strategy.partitioner` | Yes | Output partition path strategy | `UNPARTITIONED`, `EVENT_TIME`, `PROCESSING_TIME`, `RECORD_FIELD` |
| `debezium.output.avro.naming-strategy.file-name` | Yes | Output file naming strategy | `PROCESSING_TIME`, `UUID` |
| `debezium.output.avro.naming-strategy.record-partition-field` | Conditional | Source field for `RECORD_FIELD` partitioner | `lake_part` |
| `debezium.output.avro.naming-strategy.storage` | Yes | Output storage backend | `S3`, `HDFS` |
| `debezium.output.avro.naming-strategy.target-path` | Yes | Target bucket/path prefix | `warehouse` |
| `debezium.output.avro.codec` | No | Avro compression codec | `NONE`, `SNAPPY`, `DEFLATE`, `BZIP2`, `ZSTD`, `XZ` |

## Partitioner strategies

| Value | Description |
|---|---|
| `UNPARTITIONED` | Writes data into a single logical output path without date/field partitions. |
| `EVENT_TIME` | Builds partition paths from event timestamps extracted from CDC records. |
| `PROCESSING_TIME` | Builds partition paths from writer processing time (ingest-time partitioning). |
| `RECORD_FIELD` | Builds partition paths from a record field specified by `record-partition-field`. |

## File-name strategies

| Value | Description |
|---|---|
| `PROCESSING_TIME` | Uses processing-time based names that are sortable by ingestion time. |
| `UUID` | Uses random UUID-based file names to avoid collisions across parallel writers. |
