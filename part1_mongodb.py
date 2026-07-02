"""
AUCA Big Data Analytics — Part 1a: MongoDB Implementation
==========================================================
Covers:
  - Schema design & justification
  - Data loading (users, products, transactions)
  - Two aggregation pipelines:
      A) Product popularity (top-selling + most-viewed)
      B) User segmentation by age group and spend

Prerequisites:
  pip install pymongo
  MongoDB running on localhost:27017
  python dataset_generator.py  (data must exist in ecommerce_data/)

Run: python part1_mongodb.py
"""

import json, os, time
from pymongo import MongoClient, ASCENDING, DESCENDING
from pymongo.errors import BulkWriteError

# ── Connection ────────────────────────────────────────────────────────────────
client = MongoClient("mongodb://localhost:27017/")
db     = client["auca_ecommerce"]
DATA   = "ecommerce_data"

print("\n" + "="*60)
print("  AUCA Big Data — Part 1a: MongoDB")
print("="*60)

# ════════════════════════════════════════════════════════════════
# SCHEMA DESIGN NOTES (embedded in code for report reference)
# ════════════════════════════════════════════════════════════════
"""
SCHEMA DESIGN DECISIONS
-----------------------

Collection: users
  Why MongoDB: User profiles are document-oriented — each user
  has a geo_data sub-document and a preferred_payment field.
  We embed a short summary of purchase history (total_orders,
  total_spend) to avoid joins on the hot path of "show user
  profile with stats". Full transaction history lives in the
  transactions collection to avoid unbounded array growth.

Collection: products
  Why MongoDB: Products have a variable price_history array
  and nested category/subcategory. The document model naturally
  represents this hierarchy. Embedding price_history avoids a
  separate price_history collection for typical read queries.

Collection: transactions
  Why MongoDB: Transactions embed their line items (items[]).
  This is an "atomic" document — a transaction and all its
  items are written and read together, so embedding avoids
  multi-document reads. The session_id foreign key links back
  to HBase sessions for cross-system queries.

Collection: categories
  Why MongoDB: Hierarchical structure (category → subcategories[])
  maps perfectly to a nested document. Rarely updated; frequently
  read for product catalog display.

What goes in HBase instead:
  Session data (see part1_hbase.py). Sessions are high-volume,
  time-series, sparse (not every session has cart contents), and
  are accessed by user_id + time range — a classic wide-column
  use case where HBase's row key design outperforms MongoDB
  document scans.
"""

# ════════════════════════════════════════════════════════════════
# LOAD DATA
# ════════════════════════════════════════════════════════════════

def load_json(filename):
    path = f"{DATA}/{filename}"
    with open(path) as f:
        return json.load(f)

def upsert_collection(collection, docs, id_field):
    """Insert docs, skip duplicates."""
    inserted = 0
    for doc in docs:
        result = collection.update_one(
            {id_field: doc[id_field]},
            {"$set": doc},
            upsert=True
        )
        inserted += result.upserted_id is not None
    return inserted

# ── Categories ────────────────────────────────────────────────────
print("\n[Loading] Categories...")
db.categories.drop()
cats = load_json("categories.json")
db.categories.insert_many(cats)
db.categories.create_index("category_id", unique=True)
print(f"  ✓ {db.categories.count_documents({})} categories loaded")

# ── Users ────────────────────────────────────────────────────────
print("[Loading] Users...")
db.users.drop()
users = load_json("users.json")
# Fill missing emails so the unique index won't collide on null
for u in users:
    if not u.get("email"):
        u["email"] = f"{u['user_id']}@placeholder.local"
    u["email"] = u["email"].lower().strip()
db.users.insert_many(users)
db.users.create_index("user_id", unique=True)
# sparse=True makes the index skip docs where email is null/missing
db.users.create_index("email", unique=True, sparse=True)
db.users.create_index([("geo_data.state", ASCENDING)])
db.users.create_index("age")
print(f"  ✓ {db.users.count_documents({})} users loaded")

# ── Products ──────────────────────────────────────────────────────
print("[Loading] Products...")
db.products.drop()
products = load_json("products.json")
db.products.insert_many(products)
db.products.create_index("product_id",  unique=True)
db.products.create_index("category_id")
db.products.create_index("is_active")
db.products.create_index([("base_price", ASCENDING)])
print(f"  ✓ {db.products.count_documents({})} products loaded")

# ── Transactions ──────────────────────────────────────────────────
print("[Loading] Transactions...")
db.transactions.drop()
txns = load_json("transactions.json")
db.transactions.insert_many(txns)
db.transactions.create_index("transaction_id", unique=True)
db.transactions.create_index("user_id")
db.transactions.create_index("session_id")
db.transactions.create_index([("timestamp", DESCENDING)])
db.transactions.create_index("status")
print(f"  ✓ {db.transactions.count_documents({})} transactions loaded")


# ════════════════════════════════════════════════════════════════
# AGGREGATION PIPELINE A — PRODUCT POPULARITY
# ════════════════════════════════════════════════════════════════
"""
Business question:
  Which products drive the most revenue, and what is
  their average selling price vs base price?

Pipeline steps:
  1. $unwind   → explode items array (one doc per line item)
  2. $group    → sum units sold, total revenue per product
  3. $lookup   → join with products collection for name/category
  4. $addFields→ compute avg_unit_price
  5. $sort     → by revenue descending
  6. $limit    → top 15
"""
print("\n[Pipeline A] Product popularity (top 15 by revenue)...")

pipeline_product_popularity = [
    # Step 1: Only completed/shipped transactions
    {"$match": {"status": {"$in": ["completed", "shipped"]}}},

    # Step 2: Explode the items array
    {"$unwind": "$items"},

    # Step 3: Group by product
    {"$group": {
        "_id":           "$items.product_id",
        "units_sold":    {"$sum": "$items.quantity"},
        "total_revenue": {"$sum": "$items.subtotal"},
        "num_orders":    {"$sum": 1}
    }},

    # Step 4: Join product details
    {"$lookup": {
        "from":         "products",
        "localField":   "_id",
        "foreignField": "product_id",
        "as":           "product_info"
    }},
    {"$unwind": {"path": "$product_info", "preserveNullAndEmptyArrays": True}},

    # Step 5: Reshape output
    {"$project": {
        "product_id":    "$_id",
        "product_name":  "$product_info.name",
        "category_id":   "$product_info.category_id",
        "base_price":    "$product_info.base_price",
        "units_sold":    1,
        "total_revenue": {"$round": ["$total_revenue", 2]},
        "num_orders":    1,
        "avg_unit_price":{"$round": [{"$divide":["$total_revenue","$units_sold"]}, 2]}
    }},

    # Step 6: Sort and limit
    {"$sort":  {"total_revenue": -1}},
    {"$limit": 15}
]

start = time.time()
top_products = list(db.transactions.aggregate(pipeline_product_popularity))
elapsed = time.time() - start
print(f"  Query time: {elapsed*1000:.1f}ms")
print(f"\n  {'Product':<35} {'Category':<10} {'Units':>6} {'Revenue':>10} {'Avg Price':>10}")
print("  " + "-"*75)
for p in top_products:
    name = (p.get("product_name","?") or "?")[:34]
    print(f"  {name:<35} {p.get('category_id','?'):<10} "
          f"{p['units_sold']:>6} ${p['total_revenue']:>9,.2f} ${p['avg_unit_price']:>9,.2f}")


# ════════════════════════════════════════════════════════════════
# AGGREGATION PIPELINE B — USER SEGMENTATION
# ════════════════════════════════════════════════════════════════
"""
Business question:
  How do spending patterns vary by age group, and which
  payment methods are preferred by high-value customers?

Pipeline steps:
  1. $group by user_id → compute total spend, order count
  2. $lookup → join with users for age, payment method
  3. $addFields → assign age group, spend tier
  4. $group by age_group → aggregate stats
  5. $sort by avg_spend desc
"""
print("\n[Pipeline B] User segmentation by age group & spend...")

pipeline_user_segments = [
    # Step 1: Only completed orders
    {"$match": {"status": {"$ne": "refunded"}}},

    # Step 2: Aggregate per user
    {"$group": {
        "_id":         "$user_id",
        "num_orders":  {"$sum": 1},
        "total_spend": {"$sum": "$total"},
        "avg_spend":   {"$avg": "$total"},
        "max_spend":   {"$max": "$total"},
        "payments":    {"$addToSet": "$payment_method"}
    }},

    # Step 3: Join user profile
    {"$lookup": {
        "from":         "users",
        "localField":   "_id",
        "foreignField": "user_id",
        "as":           "user"
    }},
    {"$unwind": {"path": "$user", "preserveNullAndEmptyArrays": True}},

    # Step 4: Add age group and spend segment
    {"$addFields": {
        "age_group": {
            "$switch": {
                "branches": [
                    {"case": {"$lt": ["$user.age", 25]}, "then": "18-24"},
                    {"case": {"$lt": ["$user.age", 35]}, "then": "25-34"},
                    {"case": {"$lt": ["$user.age", 45]}, "then": "35-44"},
                    {"case": {"$lt": ["$user.age", 55]}, "then": "45-54"},
                ],
                "default": "55+"
            }
        },
        "spend_segment": {
            "$switch": {
                "branches": [
                    {"case": {"$gt": ["$total_spend", 1000]}, "then": "high_value"},
                    {"case": {"$gt": ["$total_spend", 300]},  "then": "mid_value"},
                ],
                "default": "low_value"
            }
        }
    }},

    # Step 5: Group by age group
    {"$group": {
        "_id":             "$age_group",
        "num_users":       {"$sum": 1},
        "avg_orders":      {"$avg": "$num_orders"},
        "avg_total_spend": {"$avg": "$total_spend"},
        "avg_order_value": {"$avg": "$avg_spend"},
        "high_value_pct":  {
            "$avg": {"$cond": [{"$eq":["$spend_segment","high_value"]}, 1, 0]}
        }
    }},

    # Step 6: Sort
    {"$addFields": {
        "avg_total_spend": {"$round": ["$avg_total_spend", 2]},
        "avg_order_value": {"$round": ["$avg_order_value", 2]},
        "avg_orders":      {"$round": ["$avg_orders", 1]},
        "high_value_pct":  {"$round": [{"$multiply":["$high_value_pct",100]}, 1]}
    }},
    {"$sort": {"avg_total_spend": -1}}
]

segments = list(db.transactions.aggregate(pipeline_user_segments))
print(f"\n  {'Age Group':<10} {'Users':>6} {'Avg Orders':>11} "
      f"{'Avg Spend':>11} {'Avg Order $':>11} {'High Value%':>12}")
print("  " + "-"*65)
for s in segments:
    print(f"  {s['_id']:<10} {s['num_users']:>6} {s['avg_orders']:>11.1f} "
          f"${s['avg_total_spend']:>10,.2f} ${s['avg_order_value']:>10,.2f} "
          f"{s['high_value_pct']:>11.1f}%")


# ════════════════════════════════════════════════════════════════
# BONUS: Revenue by category (third pipeline)
# ════════════════════════════════════════════════════════════════
print("\n[Pipeline C] Revenue by category...")

pipeline_category_revenue = [
    {"$match": {"status": {"$in": ["completed","shipped"]}}},
    {"$unwind": "$items"},
    {"$lookup": {
        "from":"products",
        "localField":"items.product_id",
        "foreignField":"product_id",
        "as":"prod"
    }},
    {"$unwind": {"path":"$prod","preserveNullAndEmptyArrays":True}},
    {"$group": {
        "_id":           "$prod.category_id",
        "total_revenue": {"$sum":"$items.subtotal"},
        "units_sold":    {"$sum":"$items.quantity"},
        "num_txns":      {"$sum":1}
    }},
    {"$project":{
        "category_id":"$_id",
        "total_revenue":{"$round":["$total_revenue",2]},
        "units_sold":1,
        "num_txns":1,
        "avg_item_revenue":{"$round":[{"$divide":["$total_revenue","$num_txns"]},2]}
    }},
    {"$sort":{"total_revenue":-1}},
    {"$limit":10}
]

cat_rev = list(db.transactions.aggregate(pipeline_category_revenue))
print(f"\n  {'Category':<12} {'Revenue':>12} {'Units':>8} {'Transactions':>14}")
print("  " + "-"*50)
for c in cat_rev:
    print(f"  {c.get('category_id','?'):<12} ${c['total_revenue']:>11,.2f} "
          f"{c['units_sold']:>8} {c['num_txns']:>14}")

print("\n" + "="*60)
print("  Part 1a (MongoDB) complete.")
print("="*60)
client.close()
