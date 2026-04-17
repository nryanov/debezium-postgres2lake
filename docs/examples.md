# Examples

The `examples/` directory contains Docker Compose stacks for running `debezium-postgres2lake` end-to-end on a local machine.

## Available stacks

| Stack                     | Format  | Catalog | Docs page                                    |
|---------------------------|---------|---------|----------------------------------------------|
| `examples/avro`           | Avro    | N/A     | [Avro](examples/avro.md)                     |
| `examples/avro-spi-s3-readiness` | Avro | N/A | [Avro + external SPI JAR](examples/avro-spi-s3-readiness.md) |
| `examples/parquet`        | Parquet | N/A     | [Parquet](examples/parquet.md)               |
| `examples/orc`            | ORC     | N/A     | [ORC](examples/orc.md)                       |
| `examples/iceberg-jdbc`   | Iceberg | JDBC    | [Iceberg JDBC](examples/iceberg-jdbc.md)     |
| `examples/iceberg-nessie` | Iceberg | Nessie  | [Iceberg Nessie](examples/iceberg-nessie.md) |
| `examples/iceberg-hive`   | Iceberg | Hive    | [Iceberg Hive](examples/iceberg-hive.md)     |
| `examples/paimon-jdbc`    | Paimon  | JDBC    | [Paimon JDBC](examples/paimon-jdbc.md)       |
| `examples/paimon-hive`    | Paimon  | Hive    | [Paimon Hive](examples/paimon-hive.md)       |

## Common flow

- Build application container images: 
```bash
   ./scripts/build-container-images.sh
```

- Start one stack:

```bash
cd examples/parquet
docker compose up -d
```

- Generate CDC input traffic:

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

## Notes
- For full details, prerequisites, and scripts, see `examples/README.md`.
- For output-specific configuration keys and examples, see:
  - [Avro format config](formats/avro.md)
  - [Parquet format config](formats/parquet.md)
  - [ORC format config](formats/orc.md)
  - [Iceberg format config](formats/iceberg.md)
  - [Paimon format config](formats/paimon.md)
