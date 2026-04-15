# Avro format

`modules/avro` writes CDC output files in Avro format.

## Configuration example

```properties
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

| Property                                                      | Required    | Description                                 | Values / examples                                                |
|---------------------------------------------------------------|-------------|---------------------------------------------|------------------------------------------------------------------|
| `quarkus.http.port`                                           | No          | HTTP/health port for app container          | `9090`                                                           |
| `debezium.output.avro.threshold.records`                      | Yes         | Flush threshold by record count             | `10`                                                             |
| `debezium.output.avro.threshold.time`                         | Yes         | Flush threshold by elapsed time             | `5m`, `30s`                                                      |
| `debezium.output.avro.file-io.properties.*`                   | No          | Hadoop/S3A file IO properties map           | `fs.s3a.endpoint=...`                                            |
| `debezium.output.avro.naming-strategy.partitioner`            | Yes         | Output partition path strategy              | `UNPARTITIONED`, `EVENT_TIME`, `PROCESSING_TIME`, `RECORD_FIELD` |
| `debezium.output.avro.naming-strategy.file-name`              | Yes         | Output file naming strategy                 | `PROCESSING_TIME`, `UUID`                                        |
| `debezium.output.avro.naming-strategy.record-partition-field` | Conditional | Source field for `RECORD_FIELD` partitioner | `lake_part`                                                      |
| `debezium.output.avro.naming-strategy.storage`                | Yes         | Output storage backend                      | `S3`, `HDFS`                                                     |
| `debezium.output.avro.naming-strategy.target-path`            | Yes         | Target bucket/path prefix                   | `warehouse`                                                      |
| `debezium.output.avro.codec`                                  | No          | Avro compression codec                      | `NONE`, `SNAPPY`, `DEFLATE`, `BZIP2`, `ZSTD`, `XZ`               |

For shared CDC engine and Avro payload serialization settings (`debezium.engine.*`, `debezium.avro.*`), see [Debezium configuration](../debezium-configuration.md).

## Partitioner strategies

| Value             | Description                                                                       |
|-------------------|-----------------------------------------------------------------------------------|
| `UNPARTITIONED`   | Writes data into a single logical output path without date/field partitions.      |
| `EVENT_TIME`      | Builds partition paths from event timestamps extracted from CDC records.          |
| `PROCESSING_TIME` | Builds partition paths from writer processing time (ingest-time partitioning).    |
| `RECORD_FIELD`    | Builds partition paths from a record field specified by `record-partition-field`. |

## File-name strategies

| Value             | Description                                                                    |
|-------------------|--------------------------------------------------------------------------------|
| `PROCESSING_TIME` | Uses processing-time based names that are sortable by ingestion time.          |
| `UUID`            | Uses random UUID-based file names to avoid collisions across parallel writers. |
