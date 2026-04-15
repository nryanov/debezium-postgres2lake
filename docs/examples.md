# Examples

The `examples/` directory contains Docker Compose stacks for running `debezium-postgres2lake` end-to-end on a local machine.

## Available stacks

| Stack | Format | Catalog | Docs page |
|---|---|---|---|
| `examples/avro` | Avro | N/A | [Avro](examples/avro.md) |
| `examples/parquet` | Parquet | N/A | [Parquet](examples/parquet.md) |
| `examples/orc` | ORC | N/A | [ORC](examples/orc.md) |
| `examples/iceberg-jdbc` | Iceberg | JDBC | [Iceberg JDBC](examples/iceberg-jdbc.md) |
| `examples/iceberg-nessie` | Iceberg | Nessie | [Iceberg Nessie](examples/iceberg-nessie.md) |
| `examples/iceberg-hive` | Iceberg | Hive | [Iceberg Hive](examples/iceberg-hive.md) |
| `examples/paimon-jdbc` | Paimon | JDBC | [Paimon JDBC](examples/paimon-jdbc.md) |
| `examples/paimon-hive` | Paimon | Hive | [Paimon Hive](examples/paimon-hive.md) |

## Common flow

1. Build application container images:

```bash
./scripts/build-container-images.sh
```

2. Start one stack:

```bash
cd examples/parquet
docker compose up -d
```

3. Generate CDC input traffic:

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

## Notes

- Run only one example at a time to avoid port conflicts.
- Some stacks include additional services (for example Hive Metastore or Nessie).
- For full details, prerequisites, and scripts, see `examples/README.md`.
