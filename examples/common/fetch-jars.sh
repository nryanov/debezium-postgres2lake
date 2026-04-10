#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "${ROOT}/ext-jars"
curl -fsSL -o "${ROOT}/ext-jars/postgresql-42.7.4.jar" \
  "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
echo "Wrote ${ROOT}/ext-jars/postgresql-42.7.4.jar"
