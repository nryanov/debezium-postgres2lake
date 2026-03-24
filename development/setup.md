# PostgreSQL

```sql
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('debezium', 'pgoutput');
CREATE PUBLICATION debezium;
ALTER PUBLICATION debezium ADD TABLE public.data;
```

# Trino
- [paimon plugin](https://repository.apache.org/content/repositories/snapshots/org/apache/paimon/paimon-trino-440/1.0-SNAPSHOT/paimon-trino-440-1.0-20241214.000317-25-plugin.tar.gz)

# Docker
```shell
sudo ln -sf $HOME/.colima/default/docker.sock /var/run/docker.sock
```