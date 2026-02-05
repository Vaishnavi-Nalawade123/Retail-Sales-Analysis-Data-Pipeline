import json
import random
import csv
from datetime import datetime, timedelta
import sys
import os


MAX_ORDERS_PER_STORE = 50
MAX_ITEMS_PER_ORDER = 5
MAX_QTY_PER_ITEM = 5

DIM_PRODUCT_CSV = "dim_data/dim_product.csv"
DIM_STORE_CSV = "dim_data/dim_store.csv"
OUTPUT_DIR = "orders"


if len(sys.argv) != 2:
    print("Usage: python generate_daily_orders.py YYYYMMDD")
    sys.exit(1)

date_id = sys.argv[1]
order_date = datetime.strptime(date_id, "%Y%m%d")


stores = []

with open(DIM_STORE_CSV, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        stores.append(row["store_id"])

if not stores:
    raise RuntimeError("dim_store.csv is empty!")


products = []

with open(DIM_PRODUCT_CSV, newline="") as f:
    reader = csv.DictReader(f)
    for row in reader:
        products.append({
            "product_id": row["product_id"],
            "category_id": row["category_id"],
            "unit_price": int(row["unit_price"])
        })

if not products:
    raise RuntimeError("dim_product.csv is empty!")


orders = []
order_counter = 1

for store_id in stores:
    num_orders = random.randint(1, MAX_ORDERS_PER_STORE)

    for _ in range(num_orders):
        num_items = random.randint(1, MAX_ITEMS_PER_ORDER)
        selected_products = random.sample(products, num_items)

        items = []
        for p in selected_products:
            items.append({
                "product_id": p["product_id"],
                "quantity": random.randint(1, MAX_QTY_PER_ITEM),
                "unit_price": p["unit_price"]
            })

        random_time = timedelta(
            hours=random.randint(0, 23),
            minutes=random.randint(0, 59),
            seconds=random.randint(0, 59)
        )

        order = {
            "order_id": f"O{date_id}{str(order_counter).zfill(5)}",
            "store_id": store_id,
            "order_timestamp": (order_date + random_time).isoformat(),
            "items": items
        }

        orders.append(order)
        order_counter += 1


os.makedirs(OUTPUT_DIR, exist_ok=True)

output_file = f"{OUTPUT_DIR}/orders_{date_id}.json"

with open(output_file, "w") as f:
    json.dump(orders, f, indent=2)

print(f"Generated {len(orders)} orders → {output_file}")

