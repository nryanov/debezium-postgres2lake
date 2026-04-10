Apache Hive 3.x in Docker loads the PostgreSQL JDBC driver from this directory via /tmp/ext-jars.

If postgresql-*.jar is missing, from the repository root run:

  bash examples/common/fetch-jars.sh
