# debezium-postgres2lake

`debezium-postgres2lake` is a Java 21 / Quarkus multi-module project that streams PostgreSQL change data capture (CDC) events with Debezium and writes them to lake-friendly storage formats.

The project is designed for local development and experimentation with CDC pipelines, object storage, and lakehouse table formats.

## Features

- PostgreSQL CDC ingestion with Debezium engine integration
- Multiple sink/output formats:
  - Avro
  - Parquet
  - ORC
  - Iceberg
  - Paimon
- Extension points for:
  - data catalog integrations (OpenMetadata, DataHub)
  - commit event emitters (Kafka)
  - readiness marker emitters (S3)
- Multi-module Gradle build with Java 21 and module-level tests
- Docker Compose example stacks for local end-to-end runs

## Project Structure

- `modules/core` - core CDC processing logic
- `modules/avro`, `modules/parquet`, `modules/orc`, `modules/iceberg`, `modules/paimon` - format-specific implementations
- `extensions/` - optional integrations and emitters
- `examples/` - runnable local demo environments
- `docs/` - MkDocs-based project documentation

## Quick Start

### 1) Build project

```bash
./gradlew build
```

### 2) Build local container images

```bash
./scripts/build-container-images.sh
```

### 3) Run an example stack

```bash
cd examples/iceberg-jdbc
docker compose up -d
```

For detailed environment setup and prerequisites, see `examples/README.md`.

## Short Examples

### Example: Run module tests

```bash
./gradlew :modules:core:test
./gradlew :modules:iceberg:build
```

### Example: Start Parquet demo

```bash
cd examples/parquet
docker compose up -d
```

### Example: Generate demo traffic in running stack

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

## Documentation

- Docs site source: `docs/`
- MkDocs config: `mkdocs.yml`
- GitHub Pages publishing workflow: `.github/workflows/docs-pages.yaml`

You can build docs locally with:

```bash
mkdocs build
```
