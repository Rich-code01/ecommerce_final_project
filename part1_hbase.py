"""
AUCA Big Data Analytics — Part 1b: HBase Implementation
========================================================
Covers:
  - Schema design & justification (row key strategy)
  - Data loading: user session time-series + product metrics
  - Queries: retrieve sessions for a user, product performance scan

Two approaches provided:
  A) HBase Shell commands (copy-paste into hbase shell)
  B) Python client via happybase (Thrift gateway)

Prerequisites for Python client:
  Docker: docker run -d -p 2181:2181 -p 9090:9090 dajobe/hbase
  pip install happybase

Run shell commands: paste into 'hbase shell'
Run Python:        python part1_hbase.py
"""

# ════════════════════════════════════════════════════════════════
# SCHEMA DESIGN NOTES
# ════════════════════════════════════════════════════════════════
SCHEMA_NOTES = """
HBase Schema Design
===================

TABLE 1: user_sessions
-----------------------
Purpose: Store user browsing sessions as a time-series stream.

Row key: {user_id}#{reverse_timestamp}
  - Reverse timestamp = (Long.MAX_VALUE - epoch_ms) so the
    most recent session for a user sorts FIRST (HBase stores
    rows in lexicographic ascending order by row key).
  - Prefix on user_id → all sessions for one user are co-located
    on the same region, enabling fast single-user range scans.
  - Example: user_000042#9223370799053008807

Column families:
  info:      (TTL=forever, VERSIONS=1)
    info:start_time     — ISO timestamp
    info:end_time       — ISO timestamp
    info:duration_secs  — integer
    info:device_type    — mobile/desktop/tablet
    info:device_os      — iOS/Android/Windows...
    info:referrer       — search_engine/social_media/...
    info:conversion     — converted/abandoned

  activity:  (TTL=forever, VERSIONS=1)
    activity:viewed     — JSON array of product_ids
    activity:cart       — JSON map of {product_id: qty}
    activity:page_count — integer

  geo:       (TTL=forever, VERSIONS=1)
    geo:city            — city name
    geo:state           — state abbreviation
    geo:ip              — IP address

Why HBase vs MongoDB for sessions:
  • Sessions are WRITE-HEAVY and HIGH-VOLUME (millions/day at scale).
  • Access pattern is almost always: "give me user X's sessions
    between time A and time B" — perfect for row key prefix scan.
  • Many columns may be empty (cart is empty for non-converters) —
    HBase's sparse wide-column model doesn't waste space on nulls.
  • MongoDB would need to scan many documents or maintain large
    user-embedded arrays; HBase row key scans are O(1) seek + O(n) read.

TABLE 2: product_metrics
------------------------
Purpose: Track product view and purchase counts per day.

Row key: {product_id}#{date}
  - Product-first prefix allows scanning all days for one product.
  - Date in YYYY-MM-DD format for natural chronological order.
  - Example: prod_00123#2025-01-15

Column families:
  daily:   (TTL=90days, VERSIONS=1)
    daily:views         — total page views that day
    daily:cart_adds     — added to cart count
    daily:purchases     — units purchased
    daily:revenue       — revenue generated

Why HBase for product metrics:
  • Time-series incremental counters → HBase has native
    increment operations (table.incrementColumnValue).
  • Can scan prod_00123 from 2025-01-01 to 2025-03-31
    with a single efficient range scan.
  • Would be expensive in MongoDB (many small documents
    or large embedded arrays updated on every purchase).
"""
print(SCHEMA_NOTES)

# ════════════════════════════════════════════════════════════════
# PART A: HBASE SHELL COMMANDS
# ════════════════════════════════════════════════════════════════
HBASE_SHELL_COMMANDS = """
# ══════════════════════════════════════════════════════════════
# HBASE SHELL COMMANDS — paste into 'hbase shell'
# ══════════════════════════════════════════════════════════════

# 1. Create tables
create 'user_sessions',
  {NAME => 'info',     VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'activity', VERSIONS => 1, COMPRESSION => 'SNAPPY'},
  {NAME => 'geo',      VERSIONS => 1}

create 'product_metrics',
  {NAME => 'daily', VERSIONS => 1, TTL => 7776000, COMPRESSION => 'SNAPPY'}

# 2. Verify tables
list
describe 'user_sessions'
describe 'product_metrics'

# 3. Insert a sample session row
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:start_time', '2025-03-12T14:37:22'
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:end_time', '2025-03-12T14:52:41'
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:duration_secs', '919'
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:device_type', 'mobile'
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:referrer', 'search_engine'
put 'user_sessions', 'user_000042#9223370789053000000',
    'info:conversion', 'converted'
put 'user_sessions', 'user_000042#9223370789053000000',
    'activity:viewed', '["prod_00123","prod_02456"]'
put 'user_sessions', 'user_000042#9223370789053000000',
    'activity:cart', '{"prod_00123":{"quantity":2,"price":129.99}}'
put 'user_sessions', 'user_000042#9223370789053000000',
    'geo:city', 'North Michaelville'
put 'user_sessions', 'user_000042#9223370789053000000',
    'geo:state', 'WY'

# 4. Insert a product metric row
put 'product_metrics', 'prod_00123#2025-03-12',
    'daily:views', '47'
put 'product_metrics', 'prod_00123#2025-03-12',
    'daily:cart_adds', '12'
put 'product_metrics', 'prod_00123#2025-03-12',
    'daily:purchases', '5'
put 'product_metrics', 'prod_00123#2025-03-12',
    'daily:revenue', '649.95'

# 5. Get a specific row
get 'user_sessions', 'user_000042#9223370789053000000'

# 6. Scan all sessions for user_000042 (prefix scan)
scan 'user_sessions', {
  STARTROW => 'user_000042#',
  STOPROW  => 'user_000042~',
  LIMIT    => 10
}

# 7. Scan product metrics for prod_00123 (all dates)
scan 'product_metrics', {
  STARTROW => 'prod_00123#',
  STOPROW  => 'prod_00123~',
  COLUMNS  => ['daily:views', 'daily:revenue']
}

# 8. Count rows
count 'user_sessions'
count 'product_metrics'
"""
print(HBASE_SHELL_COMMANDS)

# ════════════════════════════════════════════════════════════════
# PART B: PYTHON CLIENT (happybase via Thrift)
# ════════════════════════════════════════════════════════════════
import json, os, sys, struct
from datetime import datetime

DATA_DIR = "ecommerce_data"

try:
    import happybase
    HAPPYBASE_AVAILABLE = True
except ImportError:
    HAPPYBASE_AVAILABLE = False
    print("  happybase not installed. Install with: pip install happybase")
    print("  HBase Thrift server needed: docker run -d -p 2181:2181 -p 9090:9090 dajobe/hbase")
    print("  Showing code structure only.\n")

# Helper: reverse timestamp for descending sort by recency
MAX_LONG = 9_223_372_036_854_775_807
def reverse_ts(dt_str):
    dt  = datetime.strptime(dt_str, "%Y-%m-%dT%H:%M:%S")
    ms  = int(dt.timestamp() * 1000)
    rev = MAX_LONG - ms
    return str(rev).zfill(19)

def make_session_rowkey(user_id, start_time):
    return f"{user_id}#{reverse_ts(start_time)}"

def make_product_rowkey(product_id, date_str):
    """date_str: YYYY-MM-DD"""
    return f"{product_id}#{date_str}"

def load_sessions_to_hbase(connection, limit=1000):
    """Load sessions from JSON into HBase user_sessions table."""
    table = connection.table("user_sessions")
    session_files = [
        f"{DATA_DIR}/{fn}" for fn in os.listdir(DATA_DIR)
        if fn.startswith("sessions_") and fn.endswith(".json")
    ]

    loaded = 0
    with table.batch(batch_size=200) as batch:
        for sf in session_files:
            with open(sf) as f:
                sessions = json.load(f)
            for sess in sessions[:limit]:
                rowkey = make_session_rowkey(
                    sess["user_id"], sess["start_time"]
                )
                data = {
                    # info column family
                    b"info:session_id":    sess["session_id"].encode(),
                    b"info:start_time":    sess["start_time"].encode(),
                    b"info:end_time":      sess.get("end_time","").encode(),
                    b"info:duration_secs": str(sess.get("duration_seconds",0)).encode(),
                    b"info:device_type":   (sess.get("device_profile",{}) or {}).get("type","").encode(),
                    b"info:device_os":     (sess.get("device_profile",{}) or {}).get("os","").encode(),
                    b"info:referrer":      (sess.get("referrer") or "").encode(),
                    b"info:conversion":    sess.get("conversion_status","").encode(),
                    # activity column family
                    b"activity:viewed":    json.dumps(sess.get("viewed_products",[])).encode(),
                    b"activity:cart":      json.dumps(sess.get("cart_contents",{})).encode(),
                    b"activity:page_count":str(len(sess.get("page_views",[]))).encode(),
                    # geo column family
                    b"geo:city":  (sess.get("geo_data",{}) or {}).get("city","").encode(),
                    b"geo:state": (sess.get("geo_data",{}) or {}).get("state","").encode(),
                    b"geo:ip":    (sess.get("geo_data",{}) or {}).get("ip_address","").encode(),
                }
                batch.put(rowkey.encode(), data)
                loaded += 1
            if loaded >= limit:
                break
    return loaded

def load_product_metrics(connection, limit=500):
    """Derive daily product view metrics from sessions and load into HBase."""
    table = connection.table("product_metrics")

    # Build daily view counts from session data
    daily_views   = {}  # {product_id: {date: count}}
    session_files = [
        f"{DATA_DIR}/{fn}" for fn in os.listdir(DATA_DIR)
        if fn.startswith("sessions_") and fn.endswith(".json")
    ]

    for sf in session_files[:1]:  # limit to first file for demo
        with open(sf) as f:
            sessions = json.load(f)
        for sess in sessions[:limit]:
            date = sess["start_time"][:10]
            for pid in sess.get("viewed_products", []):
                daily_views.setdefault(pid, {})
                daily_views[pid][date] = daily_views[pid].get(date, 0) + 1

    loaded = 0
    with table.batch(batch_size=200) as batch:
        for pid, dates in daily_views.items():
            for date, views in dates.items():
                rowkey = make_product_rowkey(pid, date)
                batch.put(rowkey.encode(), {
                    b"daily:views": str(views).encode(),
                    b"daily:cart_adds": "0".encode(),
                    b"daily:purchases": "0".encode(),
                    b"daily:revenue":   "0.00".encode(),
                })
                loaded += 1
    return loaded

def query_user_sessions(connection, user_id, limit=5):
    """Retrieve most recent N sessions for a user (prefix scan)."""
    table      = connection.table("user_sessions")
    start_row  = f"{user_id}#".encode()
    stop_row   = f"{user_id}~".encode()

    results = []
    for row_key, data in table.scan(
        row_start=start_row, row_stop=stop_row, limit=limit
    ):
        results.append({
            "row_key":    row_key.decode(),
            "session_id": data.get(b"info:session_id",b"").decode(),
            "start_time": data.get(b"info:start_time",b"").decode(),
            "device":     data.get(b"info:device_type",b"").decode(),
            "conversion": data.get(b"info:conversion",b"").decode(),
            "referrer":   data.get(b"info:referrer",b"").decode(),
        })
    return results

def query_product_metrics(connection, product_id,
                          date_start="2025-01-01", date_end="2025-03-31"):
    """Scan daily metrics for a product within a date range."""
    table     = connection.table("product_metrics")
    start_row = f"{product_id}#{date_start}".encode()
    stop_row  = f"{product_id}#{date_end}z".encode()

    results = []
    for row_key, data in table.scan(row_start=start_row, row_stop=stop_row):
        results.append({
            "row_key":  row_key.decode(),
            "views":    data.get(b"daily:views",b"0").decode(),
            "cart":     data.get(b"daily:cart_adds",b"0").decode(),
            "purchases":data.get(b"daily:purchases",b"0").decode(),
            "revenue":  data.get(b"daily:revenue",b"0").decode(),
        })
    return results


# ── Main execution ────────────────────────────────────────────────
if HAPPYBASE_AVAILABLE:
    try:
        connection = happybase.Connection("localhost", port=9090)
        connection.open()
        print("[HBase] Connected to HBase Thrift server")

        # Create tables (skip if exist)
        existing = [t.decode() for t in connection.tables()]
        if "user_sessions" not in existing:
            connection.create_table("user_sessions", {
                "info":     {"max_versions": 1},
                "activity": {"max_versions": 1},
                "geo":      {"max_versions": 1}
            })
            print("  ✓ Created table: user_sessions")
        if "product_metrics" not in existing:
            connection.create_table("product_metrics", {
                "daily": {"max_versions": 1, "time_to_live": 7776000}
            })
            print("  ✓ Created table: product_metrics")

        # Load data
        print("\n[Loading] Loading sessions into HBase (limit 1000)...")
        n = load_sessions_to_hbase(connection, limit=1000)
        print(f"  ✓ Loaded {n} session rows")

        print("[Loading] Loading product daily metrics...")
        n = load_product_metrics(connection, limit=500)
        print(f"  ✓ Loaded {n} product metric rows")

        # Query: user sessions
        print("\n[Query 1] Sessions for user_000042 (most recent 5):")
        sessions = query_user_sessions(connection, "user_000042", limit=5)
        for s in sessions:
            print(f"  {s['start_time']}  device={s['device']:<8}  "
                  f"conversion={s['conversion']:<10}  referrer={s['referrer']}")

        # Query: product metrics
        print("\n[Query 2] Daily views for prod_00123 (Jan–Mar 2025):")
        metrics = query_product_metrics(connection, "prod_00123")
        for m in metrics[:10]:
            print(f"  {m['row_key'][-10:]}  views={m['views']:<5}  revenue=${m['revenue']}")

        connection.close()

    except Exception as e:
        print(f"  Could not connect to HBase: {e}")
        print("  Start HBase with: docker run -d -p 2181:2181 -p 9090:9090 dajobe/hbase")
else:
    print("[Info] Install happybase + start Docker HBase to run Python queries.")
    print("       The schema design and shell commands above are the primary deliverable.")

print("\n" + "="*60)
print("  Part 1b (HBase) complete.")
print("="*60)
