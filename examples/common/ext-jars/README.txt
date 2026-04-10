Apache Hive 4.x in Docker loads the PostgreSQL JDBC driver and S3 jars from this directory via /tmp/ext-jars.

If postgresql-*.jar and/or S3 jars are missing, from the repository root run:

  bash examples/common/fetch-jars.sh
