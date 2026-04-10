--liquibase formatted sql

--changeset examples:1
CREATE TABLE IF NOT EXISTS public.demo_orders (
    id BIGSERIAL PRIMARY KEY,
    customer_id BIGINT NOT NULL,
    amount NUMERIC(12, 2) NOT NULL,
    created_at TIMESTAMP WITHOUT TIME ZONE NOT NULL DEFAULT (CURRENT_TIMESTAMP)
);

--changeset examples:2 runInTransaction:false
CREATE PUBLICATION debezium FOR TABLE public.demo_orders;

--changeset examples:3 runInTransaction:false splitStatements:false
SELECT pg_create_logical_replication_slot('debezium', 'pgoutput');
