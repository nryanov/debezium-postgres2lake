# Parquet format

`modules/parquet` writes CDC output files in Parquet format.

## Configuration example

```properties
debezium.output.parquet.threshold.records=10
debezium.output.parquet.threshold.time=5m
debezium.output.parquet.naming-strategy.partitioner=UNPARTITIONED
debezium.output.parquet.naming-strategy.file-name=PROCESSING_TIME
debezium.output.parquet.naming-strategy.storage=S3
debezium.output.parquet.naming-strategy.target-path=warehouse
debezium.output.parquet.codec=SNAPPY

debezium.output.parquet.file-io.properties.fs.s3a.endpoint=http://minio:9000
debezium.output.parquet.file-io.properties.fs.s3a.access.key=admin
debezium.output.parquet.file-io.properties.fs.s3a.secret.key=password
debezium.output.parquet.file-io.properties.fs.s3a.path.style.access=true
debezium.output.parquet.file-io.properties.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem
```

## Available Parquet configs

| Property                                                         | Required    | Description                                             | Values / examples                                                |
|------------------------------------------------------------------|-------------|---------------------------------------------------------|------------------------------------------------------------------|
| `debezium.output.parquet.threshold.records`                      | Yes         | Flush threshold by record count                         | `10`                                                             |
| `debezium.output.parquet.threshold.time`                         | Yes         | Flush threshold by elapsed time                         | `5m`, `30s`                                                      |
| `debezium.output.parquet.file-io.properties.*`                   | No          | Hadoop/S3A file IO properties map                       | `debezium.output.parquet.file-io.properties.fs.s3a.endpoint=...` |
| `debezium.output.parquet.naming-strategy.partitioner`            | Yes         | Output partition path strategy                          | `UNPARTITIONED`, `EVENT_TIME`, `PROCESSING_TIME`, `RECORD_FIELD` |
| `debezium.output.parquet.naming-strategy.file-name`              | Yes         | Output file naming strategy                             | `PROCESSING_TIME`, `UUID`                                        |
| `debezium.output.parquet.naming-strategy.record-partition-field` | Conditional | Source field for `RECORD_FIELD` partitioner             | `lake_part`                                                      |
| `debezium.output.parquet.naming-strategy.storage`                | Yes         | Output storage backend                                  | `S3`, `HDFS`                                                     |
| `debezium.output.parquet.naming-strategy.target-path`            | Yes         | Target bucket/path prefix                               | `warehouse`                                                      |
| `debezium.output.parquet.codec`                                  | No          | Parquet compression codec                               | `NONE`, `SNAPPY`, `GZIP`, `LZ4`, `ZSTD`                          |

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
