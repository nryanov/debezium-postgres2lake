#!/usr/bin/env bash
set -euo pipefail
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "${ROOT}/ext-jars"

curl -fsSL -o "${ROOT}/ext-jars/postgresql-42.7.4.jar" "https://repo1.maven.org/maven2/org/postgresql/postgresql/42.7.4/postgresql-42.7.4.jar"
curl -fsSL -o "${ROOT}/ext-jars/hadoop-aws-3.3.6.jar" "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar"
curl -fsSL -o "${ROOT}/ext-jars/aws-java-sdk-bundle-1.12.367.jar" "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.367/aws-java-sdk-bundle-1.12.367.jar"

echo "Wrote ${ROOT}/ext-jars/postgresql-42.7.4.jar"
echo "Wrote ${ROOT}/ext-jars/hadoop-aws-3.3.6.jar"
echo "Wrote ${ROOT}/ext-jars/aws-java-sdk-bundle-1.12.367.jar"
