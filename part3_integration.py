"""
AUCA Big Data Analytics — Part 3: Analytics Integration
========================================================
Integrated analytical queries combining data from:
  - MongoDB (simulated via JSON: users, products, transactions)
  - HBase    (simulated via JSON: sessions)
  - Spark    (joins, aggregations, complex computation)

Business questions answered:
  3.1  Customer Lifetime Value (CLV) estimation
  3.2  Product affinity / recommendation (collaborative filtering lite)
  3.3  Funnel conversion analysis (view → cart → purchase)
  3.4  Seasonal / weekly trend identification

Run AFTER part2_spark_processing.py (needs spark_outputs/).
  python part3_integration.py
"""

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

spark = (
    SparkSession.builder
    .appName("AUCA_Integration")
    .master("local[*]")
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "4g")
    .config("spark.driver.maxResultSize", "2g")
    .config("spark.sql.warehouse.dir", "spark-warehouse")
    .config("spark.hadoop.mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

import pandas as _pd, os as _os

def save_csv(df, folder, label=""):
    """Save Spark DataFrame via pandas — avoids Windows Hadoop NativeIO bug."""
    _os.makedirs(folder, exist_ok=True)
    out_path = f"{folder}/result.csv"
    df.toPandas().to_csv(out_path, index=False)
    if label:
        print(f"  ✓ Saved {label} -> {out_path}")
    return out_path

DATA_DIR   = "ecommerce_data"
OUT_DIR    = "spark_outputs"
INT_DIR    = "integration_outputs"
os.makedirs(INT_DIR, exist_ok=True)

print("\n" + "="*60)
print("  AUCA Big Data — Part 3: Analytics Integration")
print("="*60)

# ── Load all datasets (simulating reading from MongoDB + HBase) ──────────────
print("\n[Loading] Reading data (simulating MongoDB + HBase sources)...")

from pyspark.sql.types import StringType as _StrType2, IntegerType as _IntType2
_users_raw = spark.read.option("multiline","true").json(f"{DATA_DIR}/users.json")
for _col, _typ in [("email", _StrType2()), ("age", _IntType2()),
                   ("gender", _StrType2()), ("preferred_payment", _StrType2()),
                   ("name", _StrType2())]:
    if _col not in _users_raw.columns:
        _users_raw = _users_raw.withColumn(_col, F.lit(None).cast(_typ))

users = (
    _users_raw
    .withColumn("email",
                F.lower(F.coalesce(
                    F.col("email"),
                    F.concat(F.col("user_id"), F.lit("@placeholder.local"))
                )))
    .withColumn("registration_date",
                F.to_timestamp("registration_date","yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("reg_month", F.date_format("registration_date","yyyy-MM"))
    .withColumn("age_group",
                F.when(F.col("age").isNull(), "unknown")
                 .when(F.col("age")<25,"18-24")
                 .when(F.col("age")<35,"25-34")
                 .when(F.col("age")<45,"35-44")
                 .when(F.col("age")<55,"45-54")
                 .otherwise("55+"))
    .withColumn("city",  F.col("geo_data.city"))
    .withColumn("state", F.col("geo_data.state"))
    .drop("geo_data")
)

products = (
    spark.read.option("multiline","true").json(f"{DATA_DIR}/products.json")
    .withColumn("current_price",
                F.coalesce(F.col("current_price"), F.col("base_price")))
    .withColumn("price_tier",
                F.when(F.col("current_price")<20,"budget")
                 .when(F.col("current_price")<100,"mid-range")
                 .when(F.col("current_price")<300,"premium")
                 .otherwise("luxury"))
)

transactions = (
    spark.read.option("multiline","true").json(f"{DATA_DIR}/transactions.json")
    .withColumn("timestamp",
                F.to_timestamp("timestamp","yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("txn_date",  F.to_date("timestamp"))
    .withColumn("txn_month", F.date_format("timestamp","yyyy-MM"))
    .withColumn("txn_week",  F.date_format("timestamp","YYYY-ww"))
    .filter(F.col("total") > 0)
)

session_files = [
    f"{DATA_DIR}/{fn}" for fn in os.listdir(DATA_DIR)
    if fn.startswith("sessions_") and fn.endswith(".json")
]
sessions = (
    spark.read.option("multiline","true").json(session_files)
    .withColumn("start_time",
                F.to_timestamp("start_time","yyyy-MM-dd'T'HH:mm:ss"))
    .withColumn("device_type",   F.col("device_profile.type"))
    .withColumn("device_os",     F.col("device_profile.os"))
    .drop("device_profile")
    .filter(F.col("duration_seconds") > 0)
    # Drop heavy columns early to reduce memory pressure
    .drop("cart_contents", "page_views")
)

users.createOrReplaceTempView("users")
products.createOrReplaceTempView("products")
transactions.createOrReplaceTempView("transactions")
sessions.createOrReplaceTempView("sessions")
print("  ✓ All views registered")


# ════════════════════════════════════════════════════════════════
# 3.1  CUSTOMER LIFETIME VALUE (CLV) ESTIMATION
# ════════════════════════════════════════════════════════════════
"""
Business question:
  Which customers are our most valuable, and what drives their CLV?

Data sources used:
  - Users (MongoDB): demographics, registration date
  - Transactions (MongoDB): purchase history, totals
  - Sessions (HBase): engagement metrics (session count, avg duration)

Processing steps (Spark):
  1. Aggregate transactions per user → total spend, order count, recency
  2. Aggregate sessions per user → engagement score
  3. Join both with user profiles
  4. Score CLV = recency_weight × frequency × monetary_value
"""
print("\n[3.1] Customer Lifetime Value (CLV) estimation...")

# Step 1: Transaction metrics per user
txn_metrics = spark.sql("""
    SELECT
        user_id,
        COUNT(transaction_id)          AS order_count,
        ROUND(SUM(total), 2)           AS total_spend,
        ROUND(AVG(total), 2)           AS avg_order_value,
        MIN(txn_date)                  AS first_purchase_date,
        MAX(txn_date)                  AS last_purchase_date,
        DATEDIFF(MAX(txn_date), MIN(txn_date)) AS customer_tenure_days
    FROM transactions
    WHERE status != 'refunded'
    GROUP BY user_id
""")

# Step 2: Session engagement metrics per user (HBase simulation)
session_metrics = spark.sql("""
    SELECT
        user_id,
        COUNT(session_id)                     AS session_count,
        ROUND(AVG(duration_seconds)/60.0, 1) AS avg_session_min,
        ROUND(SUM(duration_seconds)/60.0, 1) AS total_session_min,
        SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END)
                                              AS converted_sessions,
        SUM(size(viewed_products))            AS total_products_viewed
    FROM sessions
    GROUP BY user_id
""")

# Step 3: Join everything and compute CLV score
clv = (
    users.select("user_id","age_group","gender","state","reg_month","preferred_payment")
    .join(txn_metrics,     on="user_id", how="left")
    .join(session_metrics, on="user_id", how="left")
    # Fill nulls for users with no purchases
    .fillna({"order_count":0,"total_spend":0.0,"session_count":0,
             "avg_session_min":0.0,"total_products_viewed":0})
    # CLV score: simple RFM-inspired formula
    # Recency: higher if last purchase recent (we proxy as order_count for simplicity)
    # Frequency: order_count normalized
    # Monetary: total_spend
    .withColumn("engagement_score",
        F.round(
            (F.col("session_count") * 0.4) +
            (F.col("total_products_viewed") * 0.3) +
            (F.col("converted_sessions") * 10 * 0.3),
        2))
    .withColumn("clv_score",
        F.round(
            (F.col("total_spend") * 0.5) +
            (F.col("order_count") * 20 * 0.3) +
            (F.col("engagement_score") * 0.2),
        2))
    .withColumn("clv_segment",
        F.when(F.col("clv_score") > 500,  "platinum")
         .when(F.col("clv_score") > 200,  "gold")
         .when(F.col("clv_score") > 50,   "silver")
         .otherwise("bronze"))
)

print("\n  CLV segment distribution:")
clv.groupBy("clv_segment").agg(
    F.count("*").alias("num_users"),
    F.round(F.avg("total_spend"),2).alias("avg_spend"),
    F.round(F.avg("order_count"),2).alias("avg_orders"),
    F.round(F.avg("session_count"),2).alias("avg_sessions")
).orderBy(F.desc("avg_spend")).show()

print("  Top 10 highest CLV customers:")
clv.select(
    "user_id","age_group","state","order_count",
    "total_spend","session_count","clv_score","clv_segment"
).orderBy(F.desc("clv_score")).show(10)

save_csv(clv, f"{INT_DIR}/clv_scores", "clv_scores")
print(f"  ✓ Saved to {INT_DIR}/clv_scores/")


# ════════════════════════════════════════════════════════════════
# 3.2  PRODUCT AFFINITY / RECOMMENDATION
# ════════════════════════════════════════════════════════════════
"""
Business question:
  Which products should we recommend together, and why?

Data sources:
  - Transactions (MongoDB): co-purchase data
  - Sessions (HBase): co-view data (products viewed in same session)
  - Products (MongoDB): category/price for enrichment

Processing steps (Spark):
  1. Co-purchase signal from transaction items
  2. Co-view signal from session viewed_products
  3. Combine signals with weights → affinity score
"""
print("\n[3.2] Product affinity & recommendation...")

# Signal 1: Co-purchase from transactions
txn_items = (
    transactions
    .select("transaction_id", F.explode("items").alias("item"))
    .select("transaction_id", F.col("item.product_id").alias("product_id"))
)
co_purchase = (
    txn_items.alias("a")
    .join(txn_items.alias("b"), on="transaction_id")
    .filter(F.col("a.product_id") < F.col("b.product_id"))
    .groupBy(
        F.col("a.product_id").alias("prod_a"),
        F.col("b.product_id").alias("prod_b")
    )
    .agg(F.count("*").alias("purchase_together"))
)

# Signal 2: Co-view from sessions
session_views = (
    sessions
    .select("session_id", F.explode("viewed_products").alias("product_id"))
)
co_view = (
    session_views.alias("a")
    .join(session_views.alias("b"), on="session_id")
    .filter(F.col("a.product_id") < F.col("b.product_id"))
    .groupBy(
        F.col("a.product_id").alias("prod_a"),
        F.col("b.product_id").alias("prod_b")
    )
    .agg(F.count("*").alias("viewed_together"))
)

# Combine signals
affinity = (
    co_purchase.join(co_view, on=["prod_a","prod_b"], how="outer")
    .fillna(0)
    .withColumn("affinity_score",
        F.round(
            F.col("purchase_together") * 3.0 +   # purchase signal weighted higher
            F.col("viewed_together")   * 1.0,
        2))
    .orderBy(F.desc("affinity_score"))
)

# Enrich with product names and categories
p_info = products.select("product_id","name","category_id","price_tier")

affinity_named = (
    affinity
    .join(p_info.alias("pa"), affinity.prod_a == F.col("pa.product_id"))
    .withColumnRenamed("name","name_a")
    .withColumnRenamed("category_id","cat_a")
    .withColumnRenamed("price_tier","tier_a")
    .join(p_info.alias("pb"), affinity.prod_b == F.col("pb.product_id"))
    .withColumnRenamed("name","name_b")
    .withColumnRenamed("category_id","cat_b")
    .withColumnRenamed("price_tier","tier_b")
    .select("prod_a","name_a","cat_a","tier_a",
            "prod_b","name_b","cat_b","tier_b",
            "purchase_together","viewed_together","affinity_score")
    .orderBy(F.desc("affinity_score"))
)

print("\n  Top 10 product affinity pairs:")
affinity_named.show(10, truncate=35)

save_csv(affinity_named.limit(200), f"{INT_DIR}/product_affinity", "product_affinity")





# ════════════════════════════════════════════════════════════════
# 3.3  FUNNEL CONVERSION ANALYSIS
# ════════════════════════════════════════════════════════════════
"""
Business question:
  Where do users drop off in the purchase funnel, and which
  devices/referrers convert best?

Data sources:
  - Sessions (HBase): page views, cart, conversion status
  - Transactions (MongoDB): confirmed purchases

Processing steps (Spark):
  1. Label each session with funnel stage reached
  2. Aggregate by device and referrer
  3. Join with transaction data for revenue per funnel stage
"""
print("\n[3.3] Funnel conversion analysis...")

funnel_sessions = (
    sessions
    # Drop the massive cart_contents STRUCT and page_views to free memory
    .drop("cart_contents", "page_views", "geo_data")
    .withColumn("reached_product_view",
        (F.size(F.col("viewed_products")) > 0).cast("int"))
    .withColumn("reached_cart",
        ((F.col("conversion_status").isNotNull()) &
         (F.size(F.col("viewed_products")) > 0)).cast("int"))
    .withColumn("reached_purchase",
        (F.col("conversion_status") == "converted").cast("int"))
    # Keep only needed columns to minimize memory footprint
    .select("session_id", "user_id", "device_type", "device_os",
            "referrer", "conversion_status", "duration_seconds",
            "viewed_products", "reached_product_view",
            "reached_cart", "reached_purchase")
)
funnel_sessions.agg(
    F.count("session_id").alias("1_total_sessions"),
    F.sum("reached_product_view").alias("2_viewed_product"),
    F.sum("reached_cart").alias("3_added_to_cart"),
    F.sum("reached_purchase").alias("4_converted")
).show()

# Funnel by device type
print("\n  Funnel by device type:")
funnel_sessions.groupBy("device_type").agg(
    F.count("session_id").alias("sessions"),
    F.sum("reached_product_view").alias("viewed"),
    F.sum("reached_cart").alias("carted"),
    F.sum("reached_purchase").alias("converted"),
    F.round(F.sum("reached_purchase")*100.0/F.count("session_id"),2)
        .alias("conv_rate_pct")
).orderBy(F.desc("sessions")).show()

# Funnel by referrer
print("\n  Funnel by referrer:")
funnel_sessions.groupBy("referrer").agg(
    F.count("session_id").alias("sessions"),
    F.sum("reached_purchase").alias("converted"),
    F.round(F.sum("reached_purchase")*100.0/F.count("session_id"),2)
        .alias("conv_rate_pct")
).orderBy(F.desc("conv_rate_pct")).show()

# Average revenue per converting referrer channel
print("\n  Revenue per referrer channel (joining HBase sessions → MongoDB transactions):")
rev_by_referrer = spark.sql("""
    SELECT
        s.referrer,
        COUNT(DISTINCT t.transaction_id) AS num_transactions,
        ROUND(SUM(t.total), 2)           AS total_revenue,
        ROUND(AVG(t.total), 2)           AS avg_order_value
    FROM sessions s
    JOIN transactions t ON s.session_id = t.session_id
    WHERE t.status != 'refunded'
    GROUP BY s.referrer
    ORDER BY total_revenue DESC
""")
rev_by_referrer.show()

save_csv(funnel_sessions, f"{INT_DIR}/funnel_analysis", "funnel_analysis")
save_csv(rev_by_referrer, f"{INT_DIR}/revenue_by_referrer", "revenue_by_referrer")
print(f"  ✓ Saved to {INT_DIR}/funnel_analysis/")


# ════════════════════════════════════════════════════════════════
# 3.4  SEASONAL TREND IDENTIFICATION
# ════════════════════════════════════════════════════════════════
"""
Business question:
  Are there weekly or monthly patterns in revenue, and which
  categories drive seasonal spikes?

Data sources:
  - Transactions (MongoDB): timestamped revenue
  - Products (MongoDB): category for grouping
  - Sessions (HBase): browsing volume as leading indicator

Processing steps (Spark):
  1. Weekly revenue with 4-week rolling average (trend line)
  2. Category revenue broken down by month
  3. Session volume vs conversion rate over time
"""
print("\n[3.4] Seasonal trend identification...")

# Weekly revenue with rolling 4-week average
weekly = spark.sql("""
    SELECT
        txn_week,
        COUNT(transaction_id)        AS num_txns,
        ROUND(SUM(total), 2)         AS revenue,
        COUNT(DISTINCT user_id)      AS unique_buyers
    FROM transactions
    WHERE status != 'refunded'
    GROUP BY txn_week
    ORDER BY txn_week
""")

window_4w = Window.orderBy("txn_week").rowsBetween(-3, 0)
weekly_trend = (
    weekly
    .withColumn("rolling_avg_revenue",
        F.round(F.avg("revenue").over(window_4w), 2))
    .withColumn("rolling_avg_buyers",
        F.round(F.avg("unique_buyers").over(window_4w), 1))
)
print("\n  Weekly revenue with 4-week rolling average:")
weekly_trend.show(20)

# Monthly revenue by category
# Use DataFrame API to avoid subquery column resolution issues
monthly_cat = (
    transactions
    .filter(F.col("status") != "refunded")
    .select("txn_month", F.explode("items").alias("item"))
    .select("txn_month",
            F.col("item.product_id").alias("item_product_id"),
            F.col("item.subtotal").alias("item_subtotal"))
    .join(products.select("product_id", "category_id"),
          F.col("item_product_id") == F.col("product_id"))
    .groupBy("txn_month", "category_id")
    .agg(
        F.count("*").alias("num_txns"),
        F.round(F.sum("item_subtotal"), 2).alias("revenue")
    )
    .orderBy("txn_month", F.desc("revenue"))
)
print("\n  Monthly revenue by category (first 15 rows):")
monthly_cat.show(15)

# Session volume trend (HBase data) vs conversion rate
session_weekly = spark.sql("""
    SELECT
        DATE_FORMAT(start_time, 'YYYY-ww') AS week,
        COUNT(*)  AS total_sessions,
        SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END) AS conversions,
        ROUND(
            SUM(CASE WHEN conversion_status='converted' THEN 1 ELSE 0 END)*100.0
            / COUNT(*), 2
        ) AS conv_rate_pct
    FROM sessions
    GROUP BY DATE_FORMAT(start_time, 'YYYY-ww')
    ORDER BY week
""")
print("\n  Weekly session volume & conversion rate (HBase-sourced):")
session_weekly.show(20)

save_csv(weekly_trend, f"{INT_DIR}/weekly_trend", "weekly_trend")
save_csv(monthly_cat, f"{INT_DIR}/monthly_category_revenue", "monthly_category_revenue")
save_csv(session_weekly, f"{INT_DIR}/session_weekly_trend", "session_weekly_trend")
print(f"  ✓ Saved to {INT_DIR}/")

print("\n" + "="*60)
print("  Part 3 complete.")
print("="*60)
print("""
  Integration outputs:
  ├── clv_scores/               Customer Lifetime Value scores
  ├── product_affinity/         Combined co-purchase + co-view scores
  ├── funnel_analysis/          Session-level funnel stages
  ├── revenue_by_referrer/      Revenue per acquisition channel
  ├── weekly_trend/             Revenue + rolling average
  ├── monthly_category_revenue/ Category revenue over time
  └── session_weekly_trend/     HBase session volume per week

  Next → python part4_visualizations.py
""")

spark.stop()
