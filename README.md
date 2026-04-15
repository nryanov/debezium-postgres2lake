# debezium-postgres2lake

`debezium-postgres2lake` is a Java 21 / Quarkus multi-module CDC platform for PostgreSQL-to-lake and PostgreSQL-to-lakehouse pipelines, built on the embedded Debezium engine.

It provides a production-oriented foundation for building and operating CDC processing flows from source database capture to durable analytical storage, with modular format implementations and explicit extension points for integration needs.

## Why this project exists

Teams often need strong control over CDC behavior, output formats, and metadata integrations without being locked into fixed connector stacks or managed black boxes. This project exists to provide:

- A single, auditable codebase for PostgreSQL CDC ingestion and lake/lakehouse outputs.
- A modular architecture that supports organization-specific requirements.
- A practical path from initial rollout to long-term maintainability.

## Core capabilities

- Capture PostgreSQL change events via logical replication with Debezium.
- Persist CDC output to:
  - Avro
  - Parquet
  - ORC
  - Iceberg
  - Paimon
- Extend integration behavior through SPI APIs for:
  - data catalogs (OpenMetadata, DataHub)
  - commit event emission (Kafka)
  - readiness markers (S3)
- Use current SPI implementations as reference implementations, and extend or replace them with your own modules as needed.
- Run complete example stacks (PostgreSQL, MinIO, schema registry, optional services) for validation and onboarding.

## Alternatives and project positioning

Common alternatives:

- **Kafka Connect + Debezium + sink connectors**
  - Great for production-scale distributed deployment.
  - Usually heavier for quick local iteration and custom sink logic experiments.
- **Custom ingestion scripts/jobs**
  - Fast to start, but often hard to maintain and standardize.
- **Managed ETL/CDC services**
  - Convenient operationally, but less transparent/extensible for low-level CDC behavior.

Why `debezium-postgres2lake` can be a better fit:

- End-to-end CDC processing architecture from PostgreSQL to lake/lakehouse targets in one repository.
- Direct ownership and control of pipeline behavior, extensibility, and evolution.
- Pluggable integration layer where catalog, commit, and readiness behavior can be adapted to enterprise standards.
- Clear format strategy: object-file outputs (Avro/Parquet/ORC) and table formats (Iceberg/Paimon).

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

For prerequisites, service ports, and all example stack variants, see `examples/README.md`.
Examples use Docker Compose `include:` syntax, so use Docker Compose v2.20+.

## Project layout

- `modules/core`: core CDC processing logic
- `modules/avro`, `modules/parquet`, `modules/orc`, `modules/iceberg`, `modules/paimon`: format-specific apps
- `extensions/`: optional SPI integrations
- `examples/`: runnable local environments
- `docs/`: MkDocs documentation source

## Documentation

- Docs site: [https://nryanov.github.io/debezium-postgres2lake/](https://nryanov.github.io/debezium-postgres2lake/)
- Getting started: `docs/getting-started.md`
- Examples docs: `docs/examples.md`
- Formats and extensions: `docs/formats-and-extensions.md`

Build docs locally:

```bash
mkdocs build --strict
```

## License

Apache License 2.0. See `LICENSE`.
