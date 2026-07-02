"""
AUCA Big Data Analytics — Part 2: Apache Spark Processing
=========================================================
Covers:
  2.1  Data cleaning & normalization
  2.2  Batch job: "Users who bought X also bought Y" (product co-purchase)
  2.3  Batch job: Cohort analysis of user purchasing patterns
  2.4  Spark SQL analytics on all data sources

Prerequisites:
  pip install pyspark faker pandas matplotlib seaborn
  python dataset_generator.py   (run first to generate ecommerce_data/)

Run:
  python part2_spark_processing.py
"""

import os, json
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    FloatType, BooleanType, ArrayType, MapType, TimestampType
)
from pyspark.sql.window import Window

# ── Spark session ────────────────────────────────────────────────────────────
spark = (
    SparkSession.builder
    .appName("AUCA_Ecommerce_Analytics")
    .master("local[*]")                          # use all local CPU cores
    .config("spark.sql.shuffle.partitions", "8") # keep small for local mode
    .config("spark.driver.memory", "2g")
    # Windows fix: avoid Hadoop NativeIO errors when writing files
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")          # suppress INFO noise

import pandas as _pd, os as _os

def save_csv(df, folder, label=""):
    """Save a Spark DataFrame to CSV via pandas (avoids Windows Hadoop NativeIO bug)."""
    _os.makedirs(folder, exist_ok=True)
    out_path = f"{folder}/result.csv"
    df.toPandas().to_csv(out_path, index=False)
    if label:
        print(f"  ✓ Saved {label} -> {out_path}")
    return out_path

DATA_DIR    = "ecommerce_data"
OUTPUT_DIR  = "spark_outputs"
os.makedirs(OUTPUT_DIR, exist_ok=True)

print("\n" + "="*60)
print("  AUCA Big Data — Part 2: Spark Processing")
print("="*60)

# ════════════════════════════════════════════════════════════════
# 2.1  DATA CLEANING & NORMALIZATION
# ════════════════════════════════════════════════════════════════
print("\n[2.1] Loading and cleaning raw JSON data...")

# ── Users ────────────────────────────────────────────────────────
users_raw = spark.read.option("multiline", "true").json(f"{DATA_DIR}/users.json")

# Safely add any missing optional columns before transformations
from pyspark.sql.types import StringType as _StrType, IntegerType as _IntType
for _col, _typ in [("email", _StrType()), ("age", _IntType()),
                   ("gender", _StrType()), ("preferred_payment", _StrType()),
                   ("name", _StrType())]:
    if _col not in users_raw.columns:
        users_raw = users_raw.withColumn(_col, F.lit(None).cast(_typ))

users_clean = (
    users_raw
    # Only drop rows missing truly critical fields
    .dropna(subset=["user_id", "registration_date"])
    # Extract nested geo fields to flat columns
    .withColumn("city",    F.col("geo_data.city"))
    .withColumn("state",   F.col("geo_data.state"))
    .withColumn("country", F.col("geo_data.country"))
    # Normalize email; fill null with a generated placeholder
    .withColumn("email",
                F.lower(F.coalesce(
                    F.col("email"),
                    F.concat(F.col("user_id"), F.lit("@placeholder.local"))
                )))
    # Cast registration date string to timestamp
    .withColumn("registration_date",
                F.to_timestamp("registration_date", "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("last_active",
                F.to_timestamp("last_active", "yyyy-MM-dd'T'HH:mm:ss"))
    # Derive age group (age may be null if not in dataset)
    .withColumn("age_group",
                F.when(F.col("age").isNull(), "unknown")
                 .when(F.col("age") < 25, "18-24")
                 .when(F.col("age") < 35, "25-34")
                 .when(F.col("age") < 45, "35-44")
                 .when(F.col("age") < 55, "45-54")
                 .otherwise("55+"))
    # Derive registration month for cohort analysis
    .withColumn("reg_month",
                F.date_format("registration_date", "yyyy-MM"))
    .drop("geo_data")
)
users_clean.createOrReplaceTempView("users")
print(f"  Users: {users_clean.count()} rows after cleaning")

# ── Products ─────────────────────────────────────────────────────
products_raw = spark.read.option("multiline", "true").json(f"{DATA_DIR}/products.json")

products_clean = (
    products_raw
    .dropna(subset=["product_id", "category_id", "base_price"])
    # Fill missing current_price with base_price
    .withColumn("current_price",
                F.coalesce(F.col("current_price"), F.col("base_price")))
    # Ensure non-negative stock
    .withColumn("current_stock",
                F.greatest(F.col("current_stock"), F.lit(0)))
    .withColumn("creation_date",
                F.to_timestamp("creation_date", "yyyy-MM-dd'T'HH:mm:ss"))
    # Price tier for analysis
    .withColumn("price_tier",
                F.when(F.col("current_price") < 20,   "budget")
                 .when(F.col("current_price") < 100,  "mid-range")
                 .when(F.col("current_price") < 300,  "premium")
                 .otherwise("luxury"))
)
products_clean.createOrReplaceTempView("products")
print(f"  Products: {products_clean.count()} rows after cleaning")

# ── Transactions ─────────────────────────────────────────────────
txn_raw = spark.read.option("multiline", "true").json(f"{DATA_DIR}/transactions.json")

txn_clean = (
    txn_raw
    .dropna(subset=["transaction_id", "user_id", "timestamp", "total"])
    .withColumn("timestamp",
                F.to_timestamp("timestamp", "yyyy-MM-dd'T'HH:mm:ss"))
    # Filter out clearly invalid totals
    .filter(F.col("total") > 0)
    # Normalize payment method
    .withColumn("payment_method",
                F.lower(F.trim(F.col("payment_method"))))
    # Derive date fields for time-series analysis
    .withColumn("txn_date",  F.to_date("timestamp"))
    .withColumn("txn_month", F.date_format("timestamp", "yyyy-MM"))
    .withColumn("txn_week",  F.date_format("timestamp", "YYYY-ww"))
)
txn_clean.createOrReplaceTempView("transactions")
print(f"  Transactions: {txn_clean.count()} rows after cleaning")

# ── Sessions (multiple files) ─────────────────────────────────────
session_files = [
    f"{DATA_DIR}/{fn}"
    for fn in os.listdir(DATA_DIR)
    if fn.startswith("sessions_") and fn.endswith(".json")
]
sessions_raw = (
    spark.read.option("multiline", "true").json(session_files)
)
sessions_clean = (
    sessions_raw
    .dropna(subset=["session_id", "user_id", "start_time"])
    .withColumn("start_time",
                F.to_timestamp("start_time", "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("end_time",
                F.to_timestamp("end_time", "yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("device_type",  F.col("device_profile.type"))
    .withColumn("device_os",    F.col("device_profile.os"))
    .withColumn("device_browser",F.col("device_profile.browser"))
    .drop("device_profile")
    # Duration sanity: must be positive
    .filter(F.col("duration_seconds") > 0)
)
sessions_clean.createOrReplaceTempView("sessions")
print(f"  Sessions: {sessions_clean.count()} rows across {len(session_files)} files")

print("\n  ✓ All data cleaned and registered as Spark SQL views")


# ════════════════════════════════════════════════════════════════
# 2.2  BATCH JOB — PRODUCT CO-PURCHASE ("Also Bought")
# ════════════════════════════════════════════════════════════════
print("\n[2.2] Product co-purchase analysis ('Users who bought X also bought Y')...")

# Explode transaction items to get one row per (transaction, product)
txn_items = (
    txn_clean
    .select("transaction_id", "user_id", F.explode("items").alias("item"))
    .select(
        "transaction_id",
        "user_id",
        F.col("item.product_id").alias("product_id")
    )
)

# Self-join on transaction to get all product pairs bought together
co_purchase = (
    txn_items.alias("a")
    .join(txn_items.alias("b"), on="transaction_id")
    .filter(F.col("a.product_id") < F.col("b.product_id"))  # avoid duplicates & self-pairs
    .select(
        F.col("a.product_id").alias("product_a"),
        F.col("b.product_id").alias("product_b")
    )
    .groupBy("product_a", "product_b")
    .agg(F.count("*").alias("co_purchase_count"))
    .orderBy(F.desc("co_purchase_count"))
)

# Enrich with product names
co_purchase_named = (
    co_purchase
    .join(products_clean.select("product_id", "name").alias("pa"),
          co_purchase.product_a == F.col("pa.product_id"))
    .withColumnRenamed("name", "product_a_name")
    .join(products_clean.select("product_id", "name").alias("pb"),
          co_purchase.product_b == F.col("pb.product_id"))
    .withColumnRenamed("name", "product_b_name")
    .select("product_a", "product_a_name", "product_b", "product_b_name", "co_purchase_count")
    .orderBy(F.desc("co_purchase_count"))
)

top_pairs = co_purchase_named.limit(20)
print("\n  Top 10 co-purchased product pairs:")
top_pairs.show(10, truncate=50)

# Save to CSV for report / visualization
save_csv(top_pairs, f"{OUTPUT_DIR}/co_purchase_pairs", "co_purchase_pairs")
print(f"  ✓ Saved to {OUTPUT_DIR}/co_purchase_pairs/")


# ════════════════════════════════════════════════════════════════
# 2.3  BATCH JOB — COHORT ANALYSIS
# ════════════════════════════════════════════════════════════════
print("\n[2.3] Cohort analysis — spending by registration month...")

# Join transactions → users to get registration month per transaction
txn_with_cohort = (
    txn_clean
    .join(users_clean.select("user_id", "reg_month", "age_group"), on="user_id")
    .withColumn("months_since_reg",
        F.months_between(
            F.col("txn_month").cast("string").cast("date"),
            F.to_date("reg_month", "yyyy-MM")
        ).cast(IntegerType())
    )
)

# Cohort spending table: rows = reg_month, cols = months_since_registration
cohort_spending = (
    txn_with_cohort
    .groupBy("reg_month", "months_since_reg")
    .agg(
        F.count("transaction_id").alias("num_transactions"),
        F.sum("total").alias("total_revenue"),
        F.avg("total").alias("avg_order_value"),
        F.countDistinct("user_id").alias("active_users")
    )
    .orderBy("reg_month", "months_since_reg")
)

print("\n  Cohort spending (first 15 rows):")
cohort_spending.show(15)

# Pivot: revenue per cohort over time
cohort_pivot = (
    cohort_spending
    .filter(F.col("months_since_reg") >= 0)
    .groupBy("reg_month")
    .pivot("months_since_reg", [0, 1, 2])
    .agg(F.round(F.sum("total_revenue"), 2))
    .orderBy("reg_month")
)
print("\n  Revenue pivot (cohort × month offset):")
cohort_pivot.show()

save_csv(cohort_spending, f"{OUTPUT_DIR}/cohort_analysis", "cohort_analysis")
print(f"  ✓ Saved to {OUTPUT_DIR}/cohort_analysis/")


# ════════════════════════════════════════════════════════════════
# 2.4  SPARK SQL ANALYTICS
# ════════════════════════════════════════════════════════════════
print("\n[2.4] Spark SQL analytics...")

# ── SQL Query A: Revenue by category ────────────────────────────
# Use DataFrame API to avoid Spark SQL LATERAL VIEW + JOIN restriction
from pyspark.sql.functions import explode as _explode
txn_exploded = (
    txn_clean
    .select("transaction_id", "user_id", _explode("items").alias("item"))
    .select("transaction_id", "user_id",
            F.col("item.product_id").alias("item_product_id"),
            F.col("item.subtotal").alias("item_subtotal"))
)
revenue_by_category = (
    txn_exploded
    .join(products_clean.select("product_id","category_id"),
          txn_exploded.item_product_id == products_clean.product_id)
    .groupBy("category_id")
    .agg(
        F.countDistinct("transaction_id").alias("num_transactions"),
        F.countDistinct("user_id").alias("num_customers"),
        F.round(F.sum("item_subtotal"),2).alias("total_revenue"),
        F.round(F.avg("item_subtotal"),2).alias("avg_item_revenue")
    )
    .orderBy(F.desc("total_revenue"))
)
print("\n  Revenue by category (top 10):")
revenue_by_category.show(10)
save_csv(revenue_by_category, f"{OUTPUT_DIR}/revenue_by_category", "revenue_by_category")

# ── SQL Query B: Conversion funnel ──────────────────────────────
# Use DataFrame API to avoid cart_contents STRUCT schema issue
funnel = sessions_clean.agg(
    F.count("session_id").alias("total_sessions"),
    F.sum((F.size(F.col("viewed_products")) > 0).cast("int")).alias("viewed_product"),
    F.sum(((F.col("conversion_status").isNotNull()) & (F.size(F.col("viewed_products")) > 0)).cast("int")).alias("added_to_cart"),
    F.sum((F.col("conversion_status") == "converted").cast("int")).alias("converted"),
    F.round(
        F.sum((F.col("conversion_status") == "converted").cast("int")) * 100.0
        / F.count("session_id"), 2
    ).alias("conversion_rate_pct")
)
print("\n  Conversion funnel:")
funnel.show()
save_csv(funnel, f"{OUTPUT_DIR}/conversion_funnel", "conversion_funnel")

# ── SQL Query C: Top products by views AND purchases ─────────────
product_views = spark.sql("""
    SELECT
        product_id,
        COUNT(*) AS total_views
    FROM sessions
    LATERAL VIEW EXPLODE(viewed_products) tmp AS product_id
    GROUP BY product_id
""")

product_purchases = spark.sql("""
    SELECT
        item.product_id   AS product_id,
        SUM(item.quantity)           AS units_sold,
        ROUND(SUM(item.subtotal), 2) AS revenue
    FROM (
        SELECT status, explode(items) AS item
        FROM transactions
    ) t
    WHERE t.status != 'refunded'
    GROUP BY item.product_id
""")

product_perf = (
    product_views
    .join(product_purchases, on="product_id", how="left")
    .join(products_clean.select("product_id", "name", "category_id",
                                "current_price", "price_tier"), on="product_id")
    .withColumn("view_to_purchase_rate",
        F.round(
            F.col("units_sold").cast("double") / F.col("total_views"), 4
        )
    )
    .orderBy(F.desc("revenue"))
)
print("\n  Product performance (top 10 by revenue):")
product_perf.select(
    "name", "category_id", "total_views",
    "units_sold", "revenue", "view_to_purchase_rate"
).show(10, truncate=40)
save_csv(product_perf, f"{OUTPUT_DIR}/product_performance", "product_performance")

# ── SQL Query D: User segmentation by spend ──────────────────────
user_segments = spark.sql("""
    SELECT
        u.user_id,
        u.age_group,
        u.gender,
        u.preferred_payment,
        COUNT(t.transaction_id)   AS num_orders,
        ROUND(SUM(t.total), 2)    AS lifetime_value,
        ROUND(AVG(t.total), 2)    AS avg_order_value,
        ROUND(MAX(t.total), 2)    AS max_order_value,
        MIN(t.txn_date)           AS first_purchase,
        MAX(t.txn_date)           AS last_purchase,
        CASE
            WHEN SUM(t.total) > 1000 THEN 'high_value'
            WHEN SUM(t.total) > 300  THEN 'mid_value'
            ELSE 'low_value'
        END                       AS customer_segment
    FROM users u
    LEFT JOIN transactions t ON u.user_id = t.user_id
    GROUP BY u.user_id, u.age_group, u.gender, u.preferred_payment
""")
print("\n  User segments (sample 10):")
user_segments.show(10)
save_csv(user_segments, f"{OUTPUT_DIR}/user_segments", "user_segments")

# ── SQL Query E: Weekly revenue trend ────────────────────────────
weekly_revenue = spark.sql("""
    SELECT
        txn_week,
        COUNT(transaction_id)         AS num_transactions,
        ROUND(SUM(total), 2)          AS total_revenue,
        ROUND(AVG(total), 2)          AS avg_order_value,
        COUNT(DISTINCT user_id)       AS unique_customers
    FROM transactions
    WHERE status != 'refunded'
    GROUP BY txn_week
    ORDER BY txn_week
""")
print("\n  Weekly revenue trend:")
weekly_revenue.show(20)
save_csv(weekly_revenue, f"{OUTPUT_DIR}/weekly_revenue", "weekly_revenue")

# ── SQL Query F: Device / referrer breakdown ──────────────────────
device_stats = spark.sql("""
    SELECT
        device_type,
        device_os,
        referrer,
        COUNT(*)  AS num_sessions,
        SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) AS conversions,
        ROUND(
            SUM(CASE WHEN conversion_status = 'converted' THEN 1 ELSE 0 END) * 100.0
            / COUNT(*), 2
        ) AS conv_rate_pct
    FROM sessions
    GROUP BY device_type, device_os, referrer
    ORDER BY num_sessions DESC
""")
print("\n  Conversion rate by device & referrer:")
device_stats.show(20)
save_csv(device_stats, f"{OUTPUT_DIR}/device_referrer_stats", "device_referrer_stats")

# ════════════════════════════════════════════════════════════════
# SUMMARY
# ════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("  Part 2 complete. Outputs saved to:", OUTPUT_DIR)
print("="*60)
print("""
  Files generated:
  ├── co_purchase_pairs/       Product affinity pairs
  ├── cohort_analysis/         Cohort spending by reg month
  ├── revenue_by_category/     Revenue split by category
  ├── conversion_funnel/       Funnel stage counts
  ├── product_performance/     Views × purchases per product
  ├── user_segments/           CLV-based customer segments
  ├── weekly_revenue/          Week-by-week revenue trend
  └── device_referrer_stats/   Device & referrer conversion rates
  
  Next step → run part3_integration.py (analytics integration)
  Then      → run part4_visualizations.py (charts + report data)
""")

spark.stop()
