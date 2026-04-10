"""
Streamlit dashboard — DuckLake vs Delta vs Iceberg benchmark results.

Launch with:
    uv run streamlit run dashboard/app.py
"""

from __future__ import annotations

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
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

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

@st.cache_data(ttl=30)
def load_results() -> pd.DataFrame | None:
    if not RESULTS_CSV.exists():
        return None
    df = pd.read_csv(RESULTS_CSV)
    return df


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


def speedup_table(df: pd.DataFrame, baseline: str = "ducklake") -> pd.DataFrame:
    """For each query, compute speedup ratio vs the baseline format."""
    agg = avg_by(df, "category", "query_name", "format")
    pivot = agg.pivot(index=["category", "query_name"], columns="format", values="avg_ms").reset_index()
    if baseline not in pivot.columns:
        return pivot
    for col in [c for c in pivot.columns if c not in ("category", "query_name")]:
        pivot[f"{col}_ratio"] = pivot[col] / pivot[baseline]
    return pivot


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
# Header KPIs
# ---------------------------------------------------------------------------
st.title("🦆 DuckLake · Delta · Iceberg — Benchmark Dashboard")
st.caption(f"Data: {len(df):,} benchmark records across {df['format'].nunique()} formats")

kpi_cols = st.columns(len(available_formats))
for col, fmt in zip(kpi_cols, available_formats):
    total_queries = df[df["format"] == fmt]["query_name"].nunique()
    avg_total = df[df["format"] == fmt]["elapsed_ms"].mean()
    col.metric(label=f"**{fmt}** — avg latency", value=f"{avg_total:,.1f} ms", delta=None)

st.divider()

# ---------------------------------------------------------------------------
# Tab layout
# ---------------------------------------------------------------------------
tab_overview, tab_reads, tab_aggs, tab_updates, tab_schema, tab_dbt, tab_raw = st.tabs([
    "📊 Overview",
    "📖 Reads",
    "🔢 Aggregations",
    "✏️ Updates",
    "🔧 Schema Evolution",
    "🧮 dbt",
    "🗂️ Raw Data",
])

# ---------------------------------------------------------------------------
# Overview tab
# ---------------------------------------------------------------------------
with tab_overview:
    st.subheader("Average latency by category and format")

    cat_agg = avg_by(filtered, "category", "format")
    fig = px.bar(
        cat_agg,
        x="category",
        y="avg_ms",
        color="format",
        barmode="group",
        color_discrete_map=FORMAT_COLORS,
        labels={"avg_ms": "Avg latency (ms)", "category": "Category", "format": "Format"},
        height=400,
    )
    fig.update_layout(legend_title_text="Format")
    st.plotly_chart(fig, use_container_width=True)

    st.subheader("Query-level heatmap (avg ms)")

    query_agg = avg_by(filtered, "query_name", "format")
    pivot_heat = query_agg.pivot(index="query_name", columns="format", values="avg_ms")
    fig_heat = px.imshow(
        pivot_heat,
        color_continuous_scale="RdYlGn_r",
        text_auto=".0f",
        aspect="auto",
        labels={"color": "avg ms"},
    )
    fig_heat.update_layout(height=max(350, len(pivot_heat) * 35))
    st.plotly_chart(fig_heat, use_container_width=True)

    st.subheader("Speedup vs DuckLake (lower is better for competitors)")
    sp = speedup_table(filtered, baseline="ducklake")
    ratio_cols = [c for c in sp.columns if c.endswith("_ratio") and "ducklake" not in c]
    if ratio_cols:
        sp_melted = sp[["category", "query_name"] + ratio_cols].melt(
            id_vars=["category", "query_name"],
            var_name="format_ratio",
            value_name="ratio",
        )
        sp_melted["format"] = sp_melted["format_ratio"].str.replace("_ratio", "")
        fig_sp = px.scatter(
            sp_melted,
            x="query_name",
            y="ratio",
            color="format",
            color_discrete_map=FORMAT_COLORS,
            hover_data=["category"],
            labels={"ratio": "Latency ratio vs DuckLake", "query_name": "Query"},
            height=400,
        )
        fig_sp.add_hline(y=1.0, line_dash="dash", line_color="gray", annotation_text="DuckLake baseline")
        fig_sp.update_xaxes(tickangle=30)
        st.plotly_chart(fig_sp, use_container_width=True)
        st.caption("Values > 1 mean that format is **slower** than DuckLake; < 1 means **faster**.")

# ---------------------------------------------------------------------------
# Reads tab
# ---------------------------------------------------------------------------
with tab_reads:
    st.subheader("Read / Scan benchmarks")
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

        st.markdown("**All individual runs (scatter — spread shows variability)**")
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
# Updates tab
# ---------------------------------------------------------------------------
with tab_updates:
    st.subheader("Update / Delete / Merge benchmarks")
    st.markdown(
        """
        This is where formats diverge most significantly:
        - **DuckLake** — native SQL `UPDATE` / `DELETE` / `INSERT OR REPLACE`
        - **Delta** — Rust-native `dt.update()` / `dt.delete()` / `dt.merge()` via delta-rs
        - **Iceberg** — delete-then-append pattern via pyiceberg (no single-call UPDATE primitive)
        """
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

        # Highlight the MERGE operation separately
        merge_df = upd_df[upd_df["query_name"] == "merge_upsert_500k"]
        if not merge_df.empty:
            st.markdown("**MERGE / UPSERT detail (500K rows)**")
            m_agg = avg_by(merge_df, "format")
            fig2 = px.bar(
                m_agg, x="format", y="avg_ms", color="format",
                color_discrete_map=FORMAT_COLORS,
                labels={"avg_ms": "Avg latency (ms)", "format": "Format"},
                height=300,
            )
            fig2.update_layout(showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)

# ---------------------------------------------------------------------------
# Schema Evolution tab
# ---------------------------------------------------------------------------
with tab_schema:
    st.subheader("Schema evolution benchmarks")
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

        st.info(
            "**ADD COLUMN** in DuckLake/Delta/Iceberg is a metadata-only operation "
            "(no data rewrite). The elapsed time reflects catalog overhead only. "
            "The subsequent query tests null-handling for the new column."
        )

# ---------------------------------------------------------------------------
# dbt tab
# ---------------------------------------------------------------------------
with tab_dbt:
    st.subheader("dbt model run times by format")
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

        st.markdown("---")
        st.markdown("### dbt integration notes")
        st.markdown(
            """
            | Format | dbt profile | Source resolution | Materialization |
            |--------|------------|-------------------|-----------------|
            | **DuckLake** | `ducklake` target | `lake.<table>` (attached via DuckDB) | DuckLake tables |
            | **Delta** | `delta` target | `delta_scan('path/to/table')` | In-memory DuckDB |
            | **Iceberg** | `iceberg` target | `iceberg_scan('metadata.json')` | In-memory DuckDB |

            All three share the same dbt model SQL — only the source macro differs.
            The `{{ get_source_table('orders') }}` macro resolves to the right expression
            based on the `DBT_FORMAT` env var.
            """
        )

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
