# ORC format

`modules/orc` writes CDC output files in ORC format.

## Configuration example

```properties
debezium.output.orc.threshold.records=10
debezium.output.orc.threshold.time=5m
debezium.output.orc.naming-strategy.partitioner=UNPARTITIONED
debezium.output.orc.naming-strategy.file-name=PROCESSING_TIME
debezium.output.orc.naming-strategy.storage=S3
debezium.output.orc.naming-strategy.target-path=warehouse
debezium.output.orc.codec=SNAPPY
debezium.output.orc.row-batch-size=1024

debezium.output.orc.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.orc.file-io.properties.fs.s3a.access.key=admin
debezium.output.orc.file-io.properties.fs.s3a.secret.key=password
debezium.output.orc.file-io.properties.fs.s3a.path.style.access=true
debezium.output.orc.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available ORC configs

| Property                                                     | Required    | Description                                             | Values / examples                                                |
|--------------------------------------------------------------|-------------|---------------------------------------------------------|------------------------------------------------------------------|
| `debezium.output.orc.threshold.records`                      | Yes         | Flush threshold by record count                         | `10`                                                             |
| `debezium.output.orc.threshold.time`                         | Yes         | Flush threshold by elapsed time                         | `5m`, `30s`                                                      |
| `debezium.output.orc.file-io.properties.*`                   | No          | Hadoop/S3A file IO properties map                       | `debezium.output.orc.file-io.properties.fs.s3a.endpoint=...`     |
| `debezium.output.orc.naming-strategy.partitioner`            | Yes         | Output partition path strategy                          | `UNPARTITIONED`, `EVENT_TIME`, `PROCESSING_TIME`, `RECORD_FIELD` |
| `debezium.output.orc.naming-strategy.file-name`              | Yes         | Output file naming strategy                             | `PROCESSING_TIME`, `UUID`                                        |
| `debezium.output.orc.naming-strategy.record-partition-field` | Conditional | Source field for `RECORD_FIELD` partitioner             | `lake_part`                                                      |
| `debezium.output.orc.naming-strategy.storage`                | Yes         | Output storage backend                                  | `S3`, `HDFS`                                                     |
| `debezium.output.orc.naming-strategy.target-path`            | Yes         | Target bucket/path prefix                               | `warehouse`                                                      |
| `debezium.output.orc.codec`                                  | No          | ORC compression codec                                   | `NONE`, `ZLIB`, `SNAPPY`, `LZO`, `LZ4`, `ZSTD`                   |
| `debezium.output.orc.row-batch-size`                         | No          | ORC writer row batch size                               | `1024` (default), `2048`                                         |

If `debezium.output.orc.row-batch-size` is not set (or set to a non-positive value), runtime default is `1024`.

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
