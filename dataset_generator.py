"""
AUCA Big Data Analytics — Dataset Generator
Generates synthetic e-commerce data: users, categories, products, sessions, transactions
Run: pip install faker pandas  →  python dataset_generator.py
"""

import json, random, os
from datetime import datetime, timedelta
from faker import Faker

fake = Faker()
random.seed(42)
Faker.seed(42)

# ── Config ──────────────────────────────────────────────────────────────────
NUM_USERS        = 500
NUM_CATEGORIES   = 25
NUM_PRODUCTS     = 500
NUM_SESSIONS     = 5000
SESSIONS_PER_FILE= 1000          # sessions split across multiple files
START_DATE       = datetime(2025, 1, 1)
END_DATE         = datetime(2025, 3, 31)
OUTPUT_DIR       = "ecommerce_data"

os.makedirs(OUTPUT_DIR, exist_ok=True)

def rand_date(start=START_DATE, end=END_DATE):
    delta = end - start
    return start + timedelta(seconds=random.randint(0, int(delta.total_seconds())))

def fmt(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%S")

# ── 1. Users ─────────────────────────────────────────────────────────────────
print("Generating users...")
users = []
user_ids = [f"user_{str(i).zfill(6)}" for i in range(NUM_USERS)]

for uid in user_ids:
    reg = rand_date(datetime(2024, 10, 1), START_DATE)
    last = rand_date(reg, END_DATE)
    users.append({
        "user_id": uid,
        "name": fake.name(),
        "email": fake.email(),
        "age": random.randint(18, 70),
        "gender": random.choice(["M", "F", "Other"]),
        "geo_data": {
            "city": fake.city(),
            "state": fake.state_abbr(),
            "country": "US"
        },
        "registration_date": fmt(reg),
        "last_active": fmt(last),
        "preferred_payment": random.choice(["credit_card", "debit_card", "paypal", "apple_pay"])
    })

with open(f"{OUTPUT_DIR}/users.json", "w") as f:
    json.dump(users, f, indent=2)
print(f"  ✓ {len(users)} users")

# ── 2. Categories ─────────────────────────────────────────────────────────────
print("Generating categories...")
category_names = [
    "Electronics", "Clothing", "Books", "Home & Garden", "Sports",
    "Toys", "Beauty", "Food & Grocery", "Automotive", "Jewelry",
    "Office Supplies", "Pet Supplies", "Music", "Movies", "Software",
    "Health", "Baby", "Travel", "Outdoors", "Gaming",
    "Art & Crafts", "Industrial", "Furniture", "Kitchen", "Tools"
]

categories = []
cat_ids = [f"cat_{str(i).zfill(3)}" for i in range(NUM_CATEGORIES)]

for i, cid in enumerate(cat_ids):
    num_subs = random.randint(2, 5)
    subcategories = []
    for j in range(num_subs):
        subcategories.append({
            "subcategory_id": f"sub_{str(i).zfill(3)}_{str(j).zfill(2)}",
            "name": fake.bs().title(),
            "profit_margin": round(random.uniform(0.10, 0.45), 2)
        })
    categories.append({
        "category_id": cid,
        "name": category_names[i],
        "subcategories": subcategories
    })

with open(f"{OUTPUT_DIR}/categories.json", "w") as f:
    json.dump(categories, f, indent=2)
print(f"  ✓ {len(categories)} categories")

# ── 3. Products ───────────────────────────────────────────────────────────────
print("Generating products...")
products = []
product_ids = [f"prod_{str(i).zfill(5)}" for i in range(NUM_PRODUCTS)]

for pid in product_ids:
    cat = random.choice(categories)
    sub = random.choice(cat["subcategories"])
    base_price = round(random.uniform(5.0, 500.0), 2)
    creation = rand_date(datetime(2024, 6, 1), START_DATE)

    # Build price history (1-3 price changes)
    price_history = []
    p = round(base_price * random.uniform(0.8, 1.3), 2)
    d = creation
    for _ in range(random.randint(1, 3)):
        price_history.append({"price": p, "date": fmt(d)})
        d = rand_date(d, END_DATE)
        p = round(p * random.uniform(0.85, 1.15), 2)

    stock = random.randint(0, 200)
    products.append({
        "product_id": pid,
        "name": fake.catch_phrase().title(),
        "category_id": cat["category_id"],
        "subcategory_id": sub["subcategory_id"],
        "base_price": base_price,
        "current_price": price_history[-1]["price"],
        "current_stock": stock,
        "is_active": stock > 0,
        "price_history": price_history,
        "creation_date": fmt(creation),
        "brand": fake.company(),
        "rating": round(random.uniform(1.0, 5.0), 1),
        "review_count": random.randint(0, 2000)
    })

with open(f"{OUTPUT_DIR}/products.json", "w") as f:
    json.dump(products, f, indent=2)
print(f"  ✓ {len(products)} products")

# ── 4. Sessions + Transactions ────────────────────────────────────────────────
print("Generating sessions and transactions...")
active_products = [p for p in products if p["is_active"]]
transactions = []
all_sessions = []

page_types = ["home", "search", "category_listing", "product_detail", "cart", "checkout"]
referrers  = ["search_engine", "social_media", "direct", "email", "affiliate", None]
devices    = [
    {"type": "mobile",  "os": "iOS",     "browser": "Safari"},
    {"type": "mobile",  "os": "Android", "browser": "Chrome"},
    {"type": "desktop", "os": "Windows", "browser": "Chrome"},
    {"type": "desktop", "os": "macOS",   "browser": "Safari"},
    {"type": "tablet",  "os": "iPadOS",  "browser": "Safari"},
]

for s_idx in range(NUM_SESSIONS):
    user = random.choice(users)
    start = rand_date()
    duration = random.randint(30, 2400)
    end = start + timedelta(seconds=duration)

    # Build page views
    viewed = random.sample(active_products, min(random.randint(1, 8), len(active_products)))
    page_views = []
    t = start
    for pv in viewed:
        page_views.append({
            "timestamp": fmt(t),
            "page_type": "product_detail",
            "product_id": pv["product_id"],
            "category_id": pv["category_id"],
            "view_duration": random.randint(10, 300)
        })
        t += timedelta(seconds=random.randint(5, 120))

    # Cart: some of the viewed products
    cart = {}
    if random.random() < 0.4:
        for p in random.sample(viewed, min(random.randint(1, 3), len(viewed))):
            cart[p["product_id"]] = {
                "quantity": random.randint(1, 4),
                "price": p["current_price"]
            }

    converted = len(cart) > 0 and random.random() < 0.55
    session_id = f"sess_{''.join(random.choices('abcdef0123456789', k=10))}"

    session = {
        "session_id": session_id,
        "user_id": user["user_id"],
        "start_time": fmt(start),
        "end_time": fmt(end),
        "duration_seconds": duration,
        "geo_data": {**user["geo_data"], "ip_address": fake.ipv4()},
        "device_profile": random.choice(devices),
        "viewed_products": [p["product_id"] for p in viewed],
        "page_views": page_views,
        "cart_contents": cart,
        "conversion_status": "converted" if converted else "abandoned",
        "referrer": random.choice(referrers)
    }
    all_sessions.append(session)

    # Transaction
    if converted and cart:
        items = []
        subtotal = 0.0
        for pid, info in cart.items():
            sub = round(info["quantity"] * info["price"], 2)
            subtotal += sub
            items.append({
                "product_id": pid,
                "quantity": info["quantity"],
                "unit_price": info["price"],
                "subtotal": sub
            })
        discount = round(subtotal * random.uniform(0, 0.15), 2) if random.random() < 0.3 else 0.0
        transactions.append({
            "transaction_id": f"txn_{''.join(random.choices('abcdef0123456789', k=12))}",
            "session_id": session_id,
            "user_id": user["user_id"],
            "timestamp": fmt(end),
            "items": items,
            "subtotal": round(subtotal, 2),
            "discount": discount,
            "total": round(subtotal - discount, 2),
            "payment_method": user["preferred_payment"],
            "status": random.choice(["completed", "completed", "completed", "shipped", "refunded"])
        })

# Write sessions in chunks
for chunk_i, start_i in enumerate(range(0, len(all_sessions), SESSIONS_PER_FILE)):
    chunk = all_sessions[start_i:start_i + SESSIONS_PER_FILE]
    with open(f"{OUTPUT_DIR}/sessions_{chunk_i}.json", "w") as f:
        json.dump(chunk, f, indent=2)
    print(f"  ✓ sessions_{chunk_i}.json ({len(chunk)} sessions)")

with open(f"{OUTPUT_DIR}/transactions.json", "w") as f:
    json.dump(transactions, f, indent=2)

print(f"\n✅ Done!")
print(f"   Sessions  : {len(all_sessions)}")
print(f"   Transactions: {len(transactions)}")
print(f"   Output dir: ./{OUTPUT_DIR}/")
print(f"\nFiles created:")
for fn in sorted(os.listdir(OUTPUT_DIR)):
    size = os.path.getsize(f"{OUTPUT_DIR}/{fn}")
    print(f"   {fn:30s}  {size/1024:.1f} KB")
