```sql
SELECT PG_CREATE_LOGICAL_REPLICATION_SLOT('debezium', 'pgoutput');
CREATE PUBLICATION debezium;
ALTER PUBLICATION debezium ADD TABLE public.data;
```