"""
AUCA Big Data Analytics — Part 4: Visualizations & Insights
============================================================
Reads the CSVs produced by Parts 2 & 3 and generates
publication-quality charts for the technical report.

Charts produced:
  4.1  Weekly revenue trend with rolling average
  4.2  Revenue by category (horizontal bar)
  4.3  Customer CLV segment distribution
  4.4  Conversion funnel (waterfall)
  4.5  Product performance: views vs revenue (scatter)
  4.6  User age-group spending heatmap
  4.7  Device-type conversion rate (grouped bar)
  4.8  Cohort retention heatmap

Run: python part4_visualizations.py
Outputs saved to: visualizations/
"""

import os, glob
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
import seaborn as sns

# ── Style ────────────────────────────────────────────────────────
sns.set_theme(style="whitegrid", palette="muted", font_scale=1.1)
plt.rcParams.update({
    "figure.dpi":     150,
    "savefig.dpi":    150,
    "savefig.bbox":   "tight",
    "axes.spines.top":    False,
    "axes.spines.right":  False,
})
COLORS = sns.color_palette("Blues_d", 8)
OUT    = "visualizations"
os.makedirs(OUT, exist_ok=True)

def load_csv(folder):
    """Load CSV from a Spark output folder (pandas-saved as result.csv)."""
    # Try result.csv first (pandas save), then part-*.csv (native Spark save)
    direct = f"{folder}/result.csv"
    if os.path.exists(direct):
        return pd.read_csv(direct)
    files = glob.glob(f"{folder}/part-*.csv")
    if files:
        return pd.read_csv(files[0])
    raise FileNotFoundError(f"No CSV found in {folder}")

print("\n" + "="*60)
print("  AUCA Big Data — Part 4: Visualizations")
print("="*60)

# ════════════════════════════════════════════════════════════════
# 4.1  WEEKLY REVENUE TREND WITH ROLLING AVERAGE
# ════════════════════════════════════════════════════════════════
print("\n[4.1] Weekly revenue trend...")
try:
    wt = load_csv("integration_outputs/weekly_trend")
    wt = wt.sort_values("txn_week")

    fig, ax1 = plt.subplots(figsize=(12, 5))
    ax2 = ax1.twinx()

    ax1.bar(range(len(wt)), wt["revenue"], color=COLORS[3], alpha=0.7, label="Weekly revenue")
    ax1.plot(range(len(wt)), wt["rolling_avg_revenue"],
             color=COLORS[6], lw=2.5, marker="o", ms=4, label="4-week rolling avg")
    ax2.plot(range(len(wt)), wt["unique_buyers"],
             color="coral", lw=1.5, ls="--", label="Unique buyers (right)")

    ax1.set_xticks(range(len(wt)))
    ax1.set_xticklabels(wt["txn_week"], rotation=45, ha="right", fontsize=9)
    ax1.set_ylabel("Revenue ($)")
    ax2.set_ylabel("Unique Buyers")
    ax1.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax1.set_title("Weekly Revenue Trend with 4-Week Rolling Average", fontsize=14, pad=12)

    lines1, labels1 = ax1.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax1.legend(lines1+lines2, labels1+labels2, loc="upper left", fontsize=9)

    plt.tight_layout()
    plt.savefig(f"{OUT}/4.1_weekly_revenue_trend.png")
    plt.close()
    print("  ✓ Saved 4.1_weekly_revenue_trend.png")
except Exception as e:
    print(f"  ! Skipped (run Spark scripts first): {e}")

# ════════════════════════════════════════════════════════════════
# 4.2  REVENUE BY CATEGORY (HORIZONTAL BAR)
# ════════════════════════════════════════════════════════════════
print("[4.2] Revenue by category...")
try:
    rc = load_csv("spark_outputs/revenue_by_category")
    rc = rc.sort_values("total_revenue", ascending=True).tail(15)

    fig, ax = plt.subplots(figsize=(10, 7))
    bars = ax.barh(rc["category_id"], rc["total_revenue"],
                   color=sns.color_palette("Blues_d", len(rc)))
    ax.bar_label(bars, labels=[f"${v:,.0f}" for v in rc["total_revenue"]],
                 padding=4, fontsize=9)
    ax.set_xlabel("Total Revenue ($)")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.set_title("Top 15 Categories by Total Revenue", fontsize=14, pad=12)
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.2_revenue_by_category.png")
    plt.close()
    print("  ✓ Saved 4.2_revenue_by_category.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
# 4.3  CLV SEGMENT DISTRIBUTION (PIE + BAR)
# ════════════════════════════════════════════════════════════════
print("[4.3] CLV segment distribution...")
try:
    clv = load_csv("integration_outputs/clv_scores")

    seg_order  = ["platinum","gold","silver","bronze"]
    seg_colors = ["#2166ac","#4393c3","#74add1","#abd9e9"]

    seg_counts = (
        clv.groupby("clv_segment")
           .agg(num_users=("user_id","count"), avg_spend=("total_spend","mean"))
           .reindex(seg_order).fillna(0)
    )

    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(13, 5))

    # Pie
    ax1.pie(
        seg_counts["num_users"],
        labels=seg_counts.index,
        colors=seg_colors,
        autopct="%1.1f%%",
        startangle=140,
        wedgeprops={"edgecolor":"white","linewidth":1.5}
    )
    ax1.set_title("Customer Distribution\nby CLV Segment", fontsize=13)

    # Bar: avg spend
    ax2.bar(seg_counts.index, seg_counts["avg_spend"],
            color=seg_colors, edgecolor="white")
    ax2.set_ylabel("Average Lifetime Spend ($)")
    ax2.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax2.set_title("Average Lifetime Spend\nby CLV Segment", fontsize=13)
    for i, (seg, row) in enumerate(seg_counts.iterrows()):
        ax2.text(i, row["avg_spend"]+2, f"${row['avg_spend']:,.0f}",
                 ha="center", fontsize=9)

    plt.suptitle("Customer Lifetime Value Segmentation", fontsize=15, y=1.01)
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.3_clv_segments.png")
    plt.close()
    print("  ✓ Saved 4.3_clv_segments.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
# 4.4  CONVERSION FUNNEL (HORIZONTAL WATERFALL)
# ════════════════════════════════════════════════════════════════
print("[4.4] Conversion funnel...")
try:
    cf = load_csv("spark_outputs/conversion_funnel")
    total   = int(cf["total_sessions"].iloc[0])
    viewed  = int(cf["viewed_product"].iloc[0])
    carted  = int(cf["added_to_cart"].iloc[0])
    conv    = int(cf["converted"].iloc[0])

    stages  = ["All sessions","Viewed product","Added to cart","Purchased"]
    values  = [total, viewed, carted, conv]
    pcts    = [100, viewed/total*100, carted/total*100, conv/total*100]
    colors  = ["#1a6faf","#2980b9","#3498db","#5dade2"]

    fig, ax = plt.subplots(figsize=(10, 5))
    bars = ax.barh(stages[::-1], values[::-1], color=colors[::-1], height=0.55)

    for bar, v, p in zip(bars, values[::-1], pcts[::-1]):
        ax.text(bar.get_width()+30, bar.get_y()+bar.get_height()/2,
                f"{v:,}  ({p:.1f}%)", va="center", fontsize=10)

    ax.set_xlabel("Number of Sessions / Users")
    ax.xaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"{x:,.0f}"))
    ax.set_title("Purchase Conversion Funnel", fontsize=14, pad=12)
    ax.set_xlim(0, total * 1.25)
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.4_conversion_funnel.png")
    plt.close()
    print("  ✓ Saved 4.4_conversion_funnel.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
# 4.5  PRODUCT PERFORMANCE: VIEWS VS REVENUE (SCATTER)
# ════════════════════════════════════════════════════════════════
print("[4.5] Product views vs revenue scatter...")
try:
    pp = load_csv("spark_outputs/product_performance")
    pp = pp.dropna(subset=["total_views","revenue"])
    pp["revenue"]     = pd.to_numeric(pp["revenue"],     errors="coerce")
    pp["total_views"] = pd.to_numeric(pp["total_views"], errors="coerce")
    pp = pp.dropna()

    tier_colors = {"budget":"#abd9e9","mid-range":"#4393c3",
                   "premium":"#2166ac","luxury":"#053061"}
    pp["color"] = pp["price_tier"].map(tier_colors).fillna("gray")

    fig, ax = plt.subplots(figsize=(10, 6))
    for tier, grp in pp.groupby("price_tier"):
        ax.scatter(grp["total_views"], grp["revenue"],
                   c=tier_colors.get(tier,"gray"), label=tier,
                   alpha=0.65, edgecolors="white", s=60)

    ax.set_xlabel("Total Product Views")
    ax.set_ylabel("Total Revenue ($)")
    ax.yaxis.set_major_formatter(mtick.FuncFormatter(lambda x,_: f"${x:,.0f}"))
    ax.set_title("Product Performance: Views vs Revenue by Price Tier", fontsize=13, pad=12)
    ax.legend(title="Price tier", framealpha=0.8)
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.5_product_views_vs_revenue.png")
    plt.close()
    print("  ✓ Saved 4.5_product_views_vs_revenue.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
# 4.6  AGE GROUP × PAYMENT METHOD SPENDING HEATMAP
# ════════════════════════════════════════════════════════════════
print("[4.6] Age group spending heatmap...")
try:
    us = load_csv("spark_outputs/user_segments")
    us["lifetime_value"] = pd.to_numeric(us.get("lifetime_value",
                           us.get("total_spend", 0)), errors="coerce").fillna(0)

    # Use gender if preferred_payment missing
    group_col = "preferred_payment" if "preferred_payment" in us.columns else "gender"
    age_col   = "age_group" if "age_group" in us.columns else None

    if age_col is None:
        raise ValueError("age_group column not found in user_segments")

    us[group_col] = us[group_col].fillna("unknown")
    us[age_col]   = us[age_col].fillna("unknown")

    age_order = [a for a in ["18-24","25-34","35-44","45-54","55+","unknown"]
                 if a in us[age_col].unique()]

    pivot = (
        us.groupby([age_col, group_col])["lifetime_value"]
          .mean()
          .unstack(fill_value=0)
          .reindex(age_order)
          .fillna(0)
    )

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="Blues",
                linewidths=0.4, cbar_kws={"label": "Avg Spend ($)"},
                ax=ax)
    ax.set_title(f"Average Spend by Age Group × {group_col.replace('_',' ').title()}",
                 fontsize=13, pad=12)
    ax.set_xlabel(group_col.replace("_", " ").title())
    ax.set_ylabel("Age Group")
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.6_age_payment_heatmap.png")
    plt.close()
    print("  ✓ Saved 4.6_age_payment_heatmap.png")
except Exception as e:
    print(f"  ! Skipped 4.6: {e}")

# ════════════════════════════════════════════════════════════════
# 4.7  DEVICE-TYPE CONVERSION RATE (GROUPED BAR)
# ════════════════════════════════════════════════════════════════
print("[4.7] Device conversion rate...")
try:
    # Re-load from integration funnel output
    fa = load_csv("integration_outputs/funnel_analysis")
    fa["reached_purchase"] = pd.to_numeric(fa["reached_purchase"], errors="coerce").fillna(0)

    dev = (
        fa.groupby("device_type")
          .agg(sessions=("session_id","count"),
               conversions=("reached_purchase","sum"))
    )
    dev["conv_rate"] = dev["conversions"] / dev["sessions"] * 100

    fig, ax = plt.subplots(figsize=(8,5))
    x = range(len(dev))
    b1 = ax.bar([i-0.2 for i in x], dev["sessions"], 0.38,
                label="Sessions", color=COLORS[2], alpha=0.85)
    ax2 = ax.twinx()
    ax2.plot(x, dev["conv_rate"], "o-", color="coral",
             lw=2, ms=7, label="Conv rate %")

    ax.set_xticks(list(x))
    ax.set_xticklabels(dev.index)
    ax.set_ylabel("Number of Sessions")
    ax2.set_ylabel("Conversion Rate (%)")
    ax2.yaxis.set_major_formatter(mtick.PercentFormatter())
    ax.set_title("Session Volume & Conversion Rate by Device Type", fontsize=13, pad=12)

    lines1, labels1 = ax.get_legend_handles_labels()
    lines2, labels2 = ax2.get_legend_handles_labels()
    ax.legend(lines1+lines2, labels1+labels2, loc="upper right", fontsize=9)
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.7_device_conversion.png")
    plt.close()
    print("  ✓ Saved 4.7_device_conversion.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
# 4.8  COHORT RETENTION HEATMAP
# ════════════════════════════════════════════════════════════════
print("[4.8] Cohort retention heatmap...")
try:
    ca = load_csv("spark_outputs/cohort_analysis")
    ca["months_since_reg"] = pd.to_numeric(ca["months_since_reg"], errors="coerce")
    ca["total_revenue"]    = pd.to_numeric(ca["total_revenue"],    errors="coerce").fillna(0)
    ca = ca[ca["months_since_reg"].between(0, 4)]

    pivot = (
        ca.groupby(["reg_month","months_since_reg"])["total_revenue"]
          .sum()
          .unstack(fill_value=0)
    )
    pivot.columns = [f"Month +{c}" for c in pivot.columns]

    fig, ax = plt.subplots(figsize=(10, 5))
    sns.heatmap(pivot, annot=True, fmt=".0f", cmap="YlOrBr",
                linewidths=0.4, cbar_kws={"label":"Revenue ($)"},
                ax=ax)
    ax.set_title("Cohort Revenue Heatmap\n(Registration Month × Months Since Registration)",
                 fontsize=12, pad=12)
    ax.set_xlabel("Months Since Registration")
    ax.set_ylabel("Registration Cohort")
    plt.tight_layout()
    plt.savefig(f"{OUT}/4.8_cohort_heatmap.png")
    plt.close()
    print("  ✓ Saved 4.8_cohort_heatmap.png")
except Exception as e:
    print(f"  ! Skipped: {e}")

# ════════════════════════════════════════════════════════════════
print("\n" + "="*60)
print("  Part 4 complete. All charts saved to: visualizations/")
print("="*60)
print("""
  4.1  Weekly revenue trend (line + bar + buyers)
  4.2  Revenue by category  (horizontal bar)
  4.3  CLV segment distribution (pie + bar)
  4.4  Conversion funnel (horizontal waterfall)
  4.5  Product views vs revenue (scatter, price tier color)
  4.6  Age group × payment method heatmap
  4.7  Device conversion rate (grouped bar + line)
  4.8  Cohort retention heatmap

  Embed these images in your Technical Report PDF.
  Next → write part1_mongodb.py and part1_hbase.py
""")