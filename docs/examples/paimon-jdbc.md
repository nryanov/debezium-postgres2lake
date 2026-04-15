# Example: Paimon JDBC catalog

Runs Paimon output with a JDBC-backed Paimon catalog stored in PostgreSQL.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/paimon-jdbc` |
| Compose file | `examples/paimon-jdbc/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-paimon:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/paimon-jdbc/config/application.properties` |
| Extra services | Base services only (`postgres`, `schema-registry`, `minio`, `jupyter`, etc.) |

## Run

```bash
cd examples/paimon-jdbc
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
