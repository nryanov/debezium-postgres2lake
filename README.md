# debezium-postgres2lake

`debezium-postgres2lake` is a Java 21 / Quarkus multi-module CDC platform for PostgreSQL-to-lake and PostgreSQL-to-lakehouse pipelines, built on the embedded Debezium engine.

It provides an opinionated, extensible foundation for running CDC ingestion flows from source database capture to durable analytical storage.

## Documentation

- Docs site: https://nryanov.com/debezium-postgres2lake/

## Quick start

From repository root:

```bash
./gradlew build
./scripts/build-container-images.sh
docker build -t local/jupyter-spark:0.1.0 -f examples/common/jupyter/Dockerfile examples/common/jupyter
cd examples/iceberg-jdbc
docker compose up -d
```

Then generate test traffic:

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

Examples use Docker Compose `include:` syntax, so use Docker Compose v2.20+.
For prerequisites and environment requirements, see `docs/requirements.md`.
