import argparse
import os
import random
import time

import psycopg


# interval between batches
interval = 1
# amount of batches to insert
batchesCount = 10
# rows in each batch
batchSize = 3

# current batch
batch = 0

with psycopg.connect("postgresql://postgres:postgres@postgres:5432/postgres", autocommit=False) as conn:
    while True:
        for _ in range(batchSize):
            conn.execute(
                "INSERT INTO public.demo_orders (customer_id, amount) VALUES (%s, %s)",
                (random.randint(1, 10_000), round(random.uniform(1.0, 500.0), 2)),
            )
        conn.commit()
        batch += 1
        print(f"Inserted batch {batch} ({batchSize} rows)")
        if batchesCount and batch >= batchesCount:
            break
        time.sleep(interval)
