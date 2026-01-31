# dataset_generator.py
import json
import random
import datetime
import uuid
import threading
import numpy as np
from faker import Faker

fake = Faker()

# ================== CONFIGURATION ==================
NUM_USERS = 10000
NUM_PRODUCTS = 5000
NUM_CATEGORIES = 25
NUM_TRANSACTIONS = 500000
NUM_SESSIONS = 2000000
TIMESPAN_DAYS = 90
CHUNK_SIZE = 100000
MAX_ITERATIONS = (NUM_SESSIONS + NUM_TRANSACTIONS) * 2

# ================== INITIALIZATION ==================
np.random.seed(42)
random.seed(42)
Faker.seed(42)

print("Initializing FULL dataset generation...")

# ================== ID GENERATORS ==================
def generate_session_id():
    return f"sess_{uuid.uuid4().hex[:10]}"

def generate_transaction_id():
    return f"txn_{uuid.uuid4().hex[:12]}"

# ================== INVENTORY MANAGER ==================
class InventoryManager:
    def __init__(self, products):
        self.products = {p["product_id"]: p for p in products}
        self.lock = threading.RLock()

    def update_stock(self, product_id, quantity):
        with self.lock:
            if self.products[product_id]["current_stock"] >= quantity:
                self.products[product_id]["current_stock"] -= quantity
                return True
            return False

    def get(self, product_id):
        with self.lock:
            return self.products.get(product_id)

# ================== CATEGORIES ==================
categories = []
for i in range(NUM_CATEGORIES):
    cat = {
        "category_id": f"cat_{i:03d}",
        "name": fake.company(),
        "subcategories": []
    }
    for j in range(random.randint(3, 5)):
        cat["subcategories"].append({
            "subcategory_id": f"sub_{i:03d}_{j:02d}",
            "name": fake.bs(),
            "profit_margin": round(random.uniform(0.1, 0.4), 2)
        })
    categories.append(cat)

print(f"Generated {len(categories)} categories")

# ================== PRODUCTS ==================
products = []
creation_start = datetime.datetime.now() - datetime.timedelta(days=TIMESPAN_DAYS * 2)

for i in range(NUM_PRODUCTS):
    category = random.choice(categories)
    base_price = round(random.uniform(5, 500), 2)

    price_history = []
    date_cursor = fake.date_time_between(start_date=creation_start, end_date="now")
    price_history.append({"price": base_price, "date": date_cursor.isoformat()})

    for _ in range(random.randint(0, 2)):
        date_cursor += datetime.timedelta(days=random.randint(10, 30))
        price_history.append({
            "price": round(base_price * random.uniform(0.8, 1.2), 2),
            "date": date_cursor.isoformat()
        })

    products.append({
        "product_id": f"prod_{i:05d}",
        "name": fake.catch_phrase().title(),
        "category_id": category["category_id"],
        "base_price": price_history[-1]["price"],
        "current_stock": random.randint(100, 2000),
        "is_active": random.random() < 0.95,
        "price_history": price_history,
        "creation_date": price_history[0]["date"]
    })

print(f"Generated {len(products)} products")

# ================== USERS ==================
users = []
for i in range(NUM_USERS):
    reg = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS*3}d", end_date=f"-{TIMESPAN_DAYS}d")
    users.append({
        "user_id": f"user_{i:06d}",
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": fake.country_code()
        },
        "registration_date": reg.isoformat(),
        "last_active": fake.date_time_between(start_date=reg, end_date="now").isoformat()
    })

print(f"Generated {len(users)} users")

inventory = InventoryManager(products)
sessions = []
transactions = []

print("Generating sessions and transactions...")

iteration = 0
while len(sessions) < NUM_SESSIONS or len(transactions) < NUM_TRANSACTIONS:
    iteration += 1
    if iteration > MAX_ITERATIONS:
        break

    user = random.choice(users)
    session_id = generate_session_id()
    start = fake.date_time_between(start_date=f"-{TIMESPAN_DAYS}d", end_date="now")
    duration = random.randint(30, 3600)

    page_views = []
    cart = {}
    viewed_products = set()

    for _ in range(random.randint(3, 15)):
        product = random.choice(products)
        viewed_products.add(product["product_id"])

        if random.random() < 0.3:
            qty = random.randint(1, 3)
            cart[product["product_id"]] = {
                "quantity": qty,
                "price": product["base_price"]
            }

        page_views.append({
            "timestamp": start.isoformat(),
            "page_type": random.choice(["home", "category_listing", "product_detail", "cart"]),
            "product_id": product["product_id"],
            "category_id": product["category_id"],
            "view_duration": random.randint(10, 120)
        })

    converted = cart and random.random() < 0.4

    sessions.append({
        "session_id": session_id,
        "user_id": user["user_id"],
        "start_time": start.isoformat(),
        "end_time": (start + datetime.timedelta(seconds=duration)).isoformat(),
        "duration_seconds": duration,
        "geo_data": {**user["geo_data"], "ip_address": fake.ipv4()},
        "device_profile": {
            "type": random.choice(["mobile", "desktop", "tablet"]),
            "os": random.choice(["iOS", "Android", "Windows", "macOS"]),
            "browser": random.choice(["Chrome", "Safari", "Firefox", "Edge"])
        },
        "viewed_products": list(viewed_products),
        "page_views": page_views,
        "cart_contents": cart,
        "conversion_status": "converted" if converted else "browsed",
        "referrer": random.choice(["direct", "email", "social", "search_engine"])
    })

    if converted and len(transactions) < NUM_TRANSACTIONS:
        items = []
        for pid, details in cart.items():
            if inventory.update_stock(pid, details["quantity"]):
                items.append({
                    "product_id": pid,
                    "quantity": details["quantity"],
                    "unit_price": details["price"],
                    "subtotal": round(details["quantity"] * details["price"], 2)
                })

        if items:
            subtotal = sum(i["subtotal"] for i in items)
            discount = round(subtotal * random.choice([0, 0.05, 0.1]), 2)
            transactions.append({
                "transaction_id": generate_transaction_id(),
                "session_id": session_id,
                "user_id": user["user_id"],
                "timestamp": start.isoformat(),
                "items": items,
                "subtotal": subtotal,
                "discount": discount,
                "total": round(subtotal - discount, 2),
                "payment_method": random.choice(["credit_card", "paypal", "apple_pay"]),
                "status": "completed"
            })

    if len(sessions) % 100000 == 0:
        print(f"{len(sessions):,} sessions | {len(transactions):,} transactions")

# ================== SAVE FILES ==================
print("Saving files...")

def dump_json(name, data):
    with open(name, "w") as f:
        json.dump(data, f)

dump_json("users.json", users)
dump_json("products.json", list(inventory.products.values()))
dump_json("categories.json", categories)
dump_json("transactions.json", transactions)

for i in range(0, len(sessions), CHUNK_SIZE):
    with open(f"sessions_{i//CHUNK_SIZE}.json", "w") as f:
        json.dump(sessions[i:i+CHUNK_SIZE], f)

print("FULL dataset generation COMPLETE.")
