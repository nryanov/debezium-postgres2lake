# Example: Paimon Hive catalog

Runs Paimon output with Hive Metastore-backed catalog services.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/paimon-hive` |
| Compose file | `examples/paimon-hive/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-paimon:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/paimon-hive/config/application.properties` |
| Extra services | Hive services from `examples/common/hive-services.yaml` |

## Run

```bash
cd examples/paimon-hive
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
