"""
Streamlit dashboard — DuckLake vs Delta vs Iceberg benchmark results.

Launch with:
    uv run streamlit run dashboard/app.py
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from pathlib import Path

# ---------------------------------------------------------------------------
# Page config
# ---------------------------------------------------------------------------
st.set_page_config(
    page_title="Table Format Benchmark",
    page_icon="🦆",
    layout="wide",
)

ROOT = Path(__file__).parent.parent
RESULTS_CSV = ROOT / "results" / "benchmark_results.csv"
DBT_RESULTS_CSV = ROOT / "results" / "dbt_benchmark_results.csv"

FORMAT_COLORS = {
    "ducklake": "#F97316",   # orange
    "delta": "#3B82F6",      # blue
    "iceberg": "#10B981",    # green
}

READ_CATEGORIES = {"reads", "aggregations"}
WRITE_CATEGORIES = {"updates"}
META_CATEGORIES = {"schema_evolution"}

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_results() -> pd.DataFrame | None:
    if not RESULTS_CSV.exists():
        return None
    return pd.read_csv(RESULTS_CSV)


@st.cache_data(ttl=30)
def load_dbt_results() -> pd.DataFrame | None:
    if not DBT_RESULTS_CSV.exists():
        return None
    return pd.read_csv(DBT_RESULTS_CSV)


def avg_by(df: pd.DataFrame, *group_cols) -> pd.DataFrame:
    return (
        df.groupby(list(group_cols))["elapsed_ms"]
        .mean()
        .reset_index()
        .rename(columns={"elapsed_ms": "avg_ms"})
    )


# ---------------------------------------------------------------------------
# Sidebar
# ---------------------------------------------------------------------------
st.sidebar.title("🦆 Format Benchmark")
st.sidebar.markdown("**DuckLake vs Delta vs Iceberg**")

if st.sidebar.button("🔄 Refresh data"):
    st.cache_data.clear()

df = load_results()
dbt_df = load_dbt_results()

if df is None:
    st.title("No benchmark results yet")
    st.info(
        "Run the benchmarks first:\n\n"
        "```bash\n"
        "uv run python scripts/01_generate_data.py\n"
        "uv run python scripts/02_setup_formats.py\n"
        "uv run python scripts/03_run_benchmarks.py\n"
        "```"
    )
    st.stop()

# Sidebar filters
available_formats = sorted(df["format"].unique())
selected_formats = st.sidebar.multiselect("Formats", available_formats, default=available_formats)

available_categories = sorted(df["category"].unique())
selected_categories = st.sidebar.multiselect("Categories", available_categories, default=available_categories)

filtered = df[df["format"].isin(selected_formats) & df["category"].isin(selected_categories)]

# ---------------------------------------------------------------------------
# Header
# ---------------------------------------------------------------------------
st.title("🦆 DuckLake · Delta · Iceberg — Local Benchmark")
st.caption(f"{len(df):,} benchmark records · {df['format'].nunique()} formats · 50M-row orders table")

st.info(
    "**What this benchmark measures (local setup)**\n\n"
    "- **Reads & Aggregations** — fair comparison: all three formats are queried by the same DuckDB engine "
    "(`delta_scan`, `iceberg_scan`, `lake.*`). Differences reflect Parquet file layout and extension overhead.\n"
    "- **Writes / Mutations** — *not* a pure format comparison: each format uses a different write library "
    "(DuckDB SQL for DuckLake, delta-rs Rust for Delta, pyiceberg Python for Iceberg). Results reflect library "
    "performance as much as format design.\n"
    "- **Schema Evolution** — metadata-only operations. Differences reflect catalog update overhead.\n\n"
    "**What this benchmark cannot measure locally:** DuckLake's catalog efficiency advantages "
    "(S3 LIST elimination, single-hop metadata vs Iceberg's 3-hop chain) are invisible on local disk — "
    "those only appear in cloud object-storage scenarios."
)

st.divider()

# ---------------------------------------------------------------------------
# KPIs — split by read vs write to avoid mixing different engines
# ---------------------------------------------------------------------------
kpi_read, kpi_write = st.columns(2)

with kpi_read:
    st.markdown("**Avg read/aggregation latency** *(same DuckDB engine — fair comparison)*")
    read_df = df[df["category"].isin(READ_CATEGORIES)]
    cols = st.columns(len(available_formats))
    for col, fmt in zip(cols, available_formats):
        val = read_df[read_df["format"] == fmt]["elapsed_ms"].mean()
        col.metric(label=fmt, value=f"{val:,.0f} ms" if pd.notna(val) else "n/a")

with kpi_write:
    st.markdown("**Avg write latency** *(different engines — compare with caution)*")
    write_df = df[df["category"].isin(WRITE_CATEGORIES)]
    cols = st.columns(len(available_formats))
    for col, fmt in zip(cols, available_formats):
        val = write_df[write_df["format"] == fmt]["elapsed_ms"].mean()
        col.metric(label=fmt, value=f"{val:,.0f} ms" if pd.notna(val) else "n/a")

st.divider()

# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------
tab_reads, tab_aggs, tab_updates, tab_schema, tab_dbt, tab_raw = st.tabs([
    "📖 Reads",
    "🔢 Aggregations",
    "✏️ Writes (library comparison)",
    "🔧 Schema Evolution",
    "🧮 dbt",
    "🗂️ Raw Data",
])

# ---------------------------------------------------------------------------
# Reads tab
# ---------------------------------------------------------------------------
with tab_reads:
    st.subheader("Read / Scan benchmarks")
    st.success(
        "All three formats are scanned by **DuckDB** (`delta_scan`, `iceberg_scan`, `lake.*`). "
        "This is the most meaningful local comparison — differences come from Parquet file layout "
        "and DuckDB extension overhead, not catalog architecture."
    )
    reads_df = filtered[filtered["category"] == "reads"]
    if reads_df.empty:
        st.info("No read benchmarks in selection.")
    else:
        r_agg = avg_by(reads_df, "query_name", "format")
        fig = px.bar(
            r_agg, x="query_name", y="avg_ms", color="format",
            barmode="group", color_discrete_map=FORMAT_COLORS,
            labels={"avg_ms": "Avg latency (ms)", "query_name": "Query"},
            height=400,
        )
        fig.update_xaxes(tickangle=20)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Query descriptions"):
            for q in reads_df["query_name"].unique():
                desc = reads_df[reads_df["query_name"] == q]["description"].iloc[0]
                st.markdown(f"- **{q}**: {desc}")

        st.markdown("**Individual runs — spread shows variability**")
        fig2 = px.strip(
            reads_df, x="query_name", y="elapsed_ms", color="format",
            color_discrete_map=FORMAT_COLORS,
            labels={"elapsed_ms": "Elapsed (ms)", "query_name": "Query"},
        )
        fig2.update_xaxes(tickangle=20)
        st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Aggregations tab
# ---------------------------------------------------------------------------
with tab_aggs:
    st.subheader("Aggregation benchmarks")
    st.success(
        "Same engine for all formats (DuckDB). Differences reflect how well each format's "
        "Parquet layout enables predicate pushdown and column pruning."
    )
    agg_df = filtered[filtered["category"] == "aggregations"]
    if agg_df.empty:
        st.info("No aggregation benchmarks in selection.")
    else:
        a_agg = avg_by(agg_df, "query_name", "format")
        fig = px.bar(
            a_agg, x="query_name", y="avg_ms", color="format",
            barmode="group", color_discrete_map=FORMAT_COLORS,
            labels={"avg_ms": "Avg latency (ms)", "query_name": "Query"},
            height=400,
        )
        fig.update_xaxes(tickangle=20)
        st.plotly_chart(fig, use_container_width=True)

        with st.expander("Query descriptions"):
            for q in agg_df["query_name"].unique():
                desc = agg_df[agg_df["query_name"] == q]["description"].iloc[0]
                st.markdown(f"- **{q}**: {desc}")

# ---------------------------------------------------------------------------
# Updates / Writes tab
# ---------------------------------------------------------------------------
with tab_updates:
    st.subheader("Write / Mutation benchmarks")
    st.warning(
        "**Caution: this tab compares write libraries, not formats.**\n\n"
        "Each format uses a completely different execution engine for mutations:\n\n"
        "| Format | Write engine | UPDATE primitive |\n"
        "|--------|-------------|------------------|\n"
        "| **DuckLake** | Native DuckDB SQL | `UPDATE` / `DELETE` / `INSERT OR REPLACE` |\n"
        "| **Delta** | delta-rs (Rust) | `dt.update()` / `dt.delete()` / `dt.merge()` |\n"
        "| **Iceberg** | pyiceberg (Python) | delete-then-append (no single-call UPDATE) |\n\n"
        "Results tell you how fast each library is locally, not how the formats would compare "
        "on a shared compute engine in the cloud."
    )
    upd_df = filtered[filtered["category"] == "updates"]
    if upd_df.empty:
        st.info("No update benchmarks in selection.")
    else:
        u_agg = avg_by(upd_df, "query_name", "format")
        fig = px.bar(
            u_agg, x="query_name", y="avg_ms", color="format",
            barmode="group", color_discrete_map=FORMAT_COLORS,
            labels={"avg_ms": "Avg latency (ms)", "query_name": "Operation"},
            height=400,
        )
        fig.update_xaxes(tickangle=10)
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# Schema Evolution tab
# ---------------------------------------------------------------------------
with tab_schema:
    st.subheader("Schema evolution benchmarks")
    st.info(
        "**ADD COLUMN** is a metadata-only operation for all three formats — no data files are rewritten. "
        "Locally, this measures catalog update overhead: a SQLite write (DuckLake), "
        "a JSON log entry (Delta), or a new metadata.json file (Iceberg). "
        "In the cloud, DuckLake's SQL catalog would have a clear edge here due to atomic transactions "
        "and no object-store round trips."
    )
    se_df = filtered[filtered["category"] == "schema_evolution"]
    if se_df.empty:
        st.info("No schema evolution benchmarks in selection.")
    else:
        s_agg = avg_by(se_df, "query_name", "format")
        fig = px.bar(
            s_agg, x="query_name", y="avg_ms", color="format",
            barmode="group", color_discrete_map=FORMAT_COLORS,
            labels={"avg_ms": "Avg latency (ms)", "query_name": "Operation"},
            height=350,
        )
        st.plotly_chart(fig, use_container_width=True)

# ---------------------------------------------------------------------------
# dbt tab
# ---------------------------------------------------------------------------
with tab_dbt:
    st.subheader("dbt model run times by format")

    st.success(
        "All three formats are queried by the same DuckDB engine via dbt-duckdb — "
        "a fair comparison, same as the Reads and Aggregations tabs. "
        "The source macro resolves to `lake.<table>`, `delta_scan(...)`, or `iceberg_scan(...)` "
        "depending on the active target."
    )

    st.warning(
        "**These models are independent workloads — they are not replicas of the direct benchmarks.**\n\n"
        "Similar names (e.g. `customer_ltv`) refer to related but different queries. "
        "Do not compare dbt model times directly against the numbers in the Reads or Aggregations tabs."
    )

    with st.expander("What each dbt model measures", expanded=True):
        st.markdown("""
| Model | Materialization | What it measures | Direct benchmark equivalent |
|---|---|---|---|
| `stg_orders` | view | View creation overhead — no data is written | — |
| `stg_customers` | view | View creation overhead | — |
| `stg_products` | view | View creation overhead | — |
| `revenue_by_region` | table | `GROUP BY region × year × month × status` on 50M rows | Similar to `revenue_by_region_month` but with 4 dimensions instead of 2 |
| `customer_ltv` | table | Join orders × customers, group by individual customer_id → 500K rows out | Named similarly to `customer_ltv` benchmark but much heavier: 500K group keys vs 40 |
| `product_performance` | table | Join orders × products + window `RANK` per category | Heavier than `top_product_categories` — adds per-product row-level detail and ranking |
| `incremental_daily_revenue` | incremental (`delete+insert`) | dbt processes only new `order_date` values not yet in the target table | **Not related to the UPDATE benchmark.** The UPDATE benchmark patches `status` on existing rows. This model appends new aggregated date rows — it is a write/materialization workload, not an in-place update. |
""")

    if dbt_df is None:
        st.info(
            "No dbt results yet. Run:\n\n"
            "```bash\n"
            "uv run python scripts/04_run_dbt_benchmarks.py\n"
            "```"
        )
    else:
        dbt_filtered = dbt_df[dbt_df["format"].isin(selected_formats)]

        d_agg = (
            dbt_filtered[dbt_filtered["model"] != "__total__"]
            .groupby(["model", "format"])["elapsed_ms"]
            .mean()
            .reset_index()
            .rename(columns={"elapsed_ms": "avg_ms"})
        )

        fig = px.bar(
            d_agg, x="model", y="avg_ms", color="format",
            barmode="group", color_discrete_map=FORMAT_COLORS,
            labels={"avg_ms": "Avg latency (ms)", "model": "dbt model"},
            height=400,
        )
        fig.update_xaxes(tickangle=20)
        st.plotly_chart(fig, use_container_width=True)

        total_agg = (
            dbt_filtered[dbt_filtered["model"] == "__total__"]
            .groupby("format")["elapsed_ms"]
            .mean()
            .reset_index()
            .rename(columns={"elapsed_ms": "total_run_ms"})
        )
        if not total_agg.empty:
            st.markdown("**Total `dbt run` wall time per format (avg across runs)**")
            cols = st.columns(len(total_agg))
            for col, (_, row) in zip(cols, total_agg.iterrows()):
                col.metric(row["format"], f"{row['total_run_ms']:,.0f} ms")

# ---------------------------------------------------------------------------
# Raw Data tab
# ---------------------------------------------------------------------------
with tab_raw:
    st.subheader("Raw benchmark records")
    st.dataframe(filtered, use_container_width=True, height=500)

    csv_download = filtered.to_csv(index=False).encode()
    st.download_button(
        "Download filtered CSV",
        data=csv_download,
        file_name="benchmark_results_filtered.csv",
        mime="text/csv",
    )

    if dbt_df is not None:
        st.subheader("Raw dbt records")
        st.dataframe(dbt_df, use_container_width=True, height=300)
