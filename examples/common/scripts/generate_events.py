#!/usr/bin/env python3
"""Insert rows into public.demo_orders for CDC examples (run from the host with Postgres port published)."""

from __future__ import annotations

import argparse
import os
import random
import time

import psycopg


def main() -> None:
    parser = argparse.ArgumentParser(description="Generate demo CDC events on demo_orders")
    parser.add_argument(
        "--database-url",
        default=os.environ.get(
            "DATABASE_URL",
            "postgresql://postgres:postgres@127.0.0.1:5432/postgres",
        ),
        help="Postgres URL for the source database (not the catalog DBs)",
    )
    parser.add_argument("--interval", type=float, default=1.0, help="Seconds between batches")
    parser.add_argument("--batch-size", type=int, default=3, help="Rows per batch")
    parser.add_argument("--batches", type=int, default=0, help="Stop after N batches (0 = run forever)")
    args = parser.parse_args()

    batch = 0
    with psycopg.connect(args.database_url, autocommit=False) as conn:
        while True:
            for _ in range(args.batch_size):
                conn.execute(
                    "INSERT INTO public.demo_orders (customer_id, amount) VALUES (%s, %s)",
                    (random.randint(1, 10_000), round(random.uniform(1.0, 500.0), 2)),
                )
            conn.commit()
            batch += 1
            print(f"Inserted batch {batch} ({args.batch_size} rows)")
            if args.batches and batch >= args.batches:
                break
            time.sleep(args.interval)


if __name__ == "__main__":
    main()
