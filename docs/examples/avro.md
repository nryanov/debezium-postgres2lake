# Example: Avro

Runs the Avro output pipeline with PostgreSQL CDC, Schema Registry, MinIO, and the Avro app image.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/avro` |
| Compose file | `examples/avro/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-avro:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/avro/config/application.properties` |
| Extra services | Base services only (`postgres`, `schema-registry`, `minio`, `jupyter`, etc.) |

## Run

```bash
cd examples/avro
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
