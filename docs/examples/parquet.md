# Example: Parquet

Runs the Parquet output pipeline with PostgreSQL CDC, Schema Registry, MinIO, and the Parquet app image.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/parquet` |
| Compose file | `examples/parquet/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-parquet:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/parquet/config/application.properties` |
| Extra services | Base services only (`postgres`, `schema-registry`, `minio`, `jupyter`, etc.) |

## Run

```bash
cd examples/parquet
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
