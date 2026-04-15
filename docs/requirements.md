# Requirements

This page describes the requirements for building and running `debezium-postgres2lake` as an end-to-end CDC processing solution.

## Runtime and build requirements

- Java 21
- Docker and Docker Compose v2.20+ (required for the provided example stacks)
- Gradle Wrapper (`./gradlew`) from repository root

Optional (documentation workflows):

- Python 3 (for helper scripts when not running them in the `jupyter` container)
- MkDocs (to build docs locally with `mkdocs build --strict`)

## Source database prerequisites (PostgreSQL)

The pipeline relies on PostgreSQL logical replication for CDC ingestion through Debezium.

At minimum, ensure your source PostgreSQL setup supports:

- Logical replication enabled
- Replication slot and publication configuration for captured tables
- Network access and credentials for the Debezium PostgreSQL connector

Project-specific Debezium connector options are configured via `debezium.engine.*` properties.

## Example environment prerequisites

For the local Docker Compose examples:

- Build project images first:

```bash
./scripts/build-container-images.sh
```

- Some examples use additional services (for example: MinIO, schema registry, catalog services, notebooks)
- Ensure required ports are free before starting a stack
- Start one example stack at a time to avoid port conflicts

For detailed stack-specific prerequisites and service notes, see:

- [Examples overview](examples.md)
- `examples/README.md`
- Individual pages under `docs/examples/`

## Recommended baseline workflow

From repository root:

```bash
./gradlew build
./scripts/build-container-images.sh
cd examples/iceberg-jdbc
docker compose up -d
```

Then generate source traffic with the helper script (inside `jupyter` container) as documented in [Getting Started](getting-started.md).
