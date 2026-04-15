# Example: ORC

Runs the ORC output pipeline with PostgreSQL CDC, Schema Registry, MinIO, and the ORC app image.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/orc` |
| Compose file | `examples/orc/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-orc:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/orc/config/application.properties` |
| Extra services | Base services only (`postgres`, `schema-registry`, `minio`, `jupyter`, etc.) |

## Run

```bash
cd examples/orc
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
