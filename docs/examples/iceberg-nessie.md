# Example: Iceberg Nessie catalog

Runs Iceberg output with a Nessie catalog service.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/iceberg-nessie` |
| Compose file | `examples/iceberg-nessie/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-iceberg:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/iceberg-nessie/config/application.properties` |
| Extra services | `nessie` (port `19120`) in addition to base services |

## Run

```bash
cd examples/iceberg-nessie
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
