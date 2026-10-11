import os
import socket
from datetime import timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from trino.auth import BasicAuthentication
from trino.dbapi import connect

import streamlit as st

# ============================================================
# CONFIGURATION

# ============================================================

TRINO_HOST = os.getenv("TRINO_HOST", "trino")

TRINO_PORT = int(os.getenv("TRINO_PORT", "8089"))

TRINO_USER = os.getenv("TRINO_USER", "airflow")

TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")

TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")

# TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "silver")

TRINO_SCHEMA = "silver"

TRINO_DQ_TABLE = os.getenv(
    "TRINO_DQ_TABLE",
    "data_quality_results",
)

FULL_TABLE_NAME = f"{TRINO_CATALOG}.{TRINO_SCHEMA}.{TRINO_DQ_TABLE}"

# ============================================================
# QUALITY DIMENSION MAPPING

# ============================================================

QUALITY_DIMENSION_MAP = {
    "missing_count": "Completeness",
    "missing_percent": "Completeness",
    "duplicate_count": "Uniqueness",
    "invalid_count": "Validity",
    "invalid_percent": "Validity",
    "freshness": "Timeliness",
    "schema": "Schema",
    "min": "Distribution",
    "max": "Distribution",
    "avg": "Distribution",
}

# ============================================================
# PAGE STYLING

# ============================================================

st.markdown(
    """
    <style>
        .dq-title {
            font-size: 2.75rem;
            font-weight: 800;
            line-height: 1.1;
            letter-spacing: -0.03em;
            color: #111827;
            margin-top: 0.2rem;
            margin-bottom: 0.35rem;
        }
        .dq-subtitle {
            color: #6b7280;
            font-size: 1rem;
            line-height: 1.5;
            margin-bottom: 0.45rem;
        }
        .dq-period {
            color: #374151;
            font-size: 0.95rem;
            margin-bottom: 1.4rem;
        }
        .dq-period strong {
            color: #111827;
        }
        .kpi-card {
            background: #ffffff;
            border: 1px solid #e5e7eb;
            border-top-width: 4px;
            border-radius: 16px;
            padding: 18px 20px;
            min-height: 135px;
            box-sizing: border-box;
            display: flex;
            flex-direction: column;
            justify-content: center;
        }
        .kpi-label {
            font-size: 0.86rem;
            font-weight: 650;
            color: #6b7280;
            margin-bottom: 10px;
            line-height: 1.2;
        }
        .kpi-value {
            font-size: 1.9rem;
            font-weight: 750;
            line-height: 1.1;
            letter-spacing: -0.03em;
            color: #111827;
            white-space: nowrap;
        }
        .kpi-subtext {
            font-size: 0.76rem;
            font-weight: 500;
            color: #9ca3af;
            margin-top: 8px;
            line-height: 1.2;
        }
        .kpi-neutral {
            border-top-color: #6366f1;
        }
        .kpi-blue {
            border-top-color: #3b82f6;
        }
        .kpi-violet {
            border-top-color: #8b5cf6;
        }
        .kpi-success {
            border-top-color: #22c55e;
            background: #f8fff9;
        }
        .kpi-success .kpi-value {
            color: #15803d;
        }
        .kpi-warning {
            border-top-color: #f59e0b;
            background: #fffdf5;
        }
        .kpi-warning .kpi-value {
            color: #b45309;
        }
        .kpi-danger {
            border-top-color: #ef4444;
            background: #fffafa;
        }
        .kpi-danger .kpi-value {
            color: #dc2626;
        }
        .kpi-good {
            border-top-color: #22c55e;
            background: #f8fff9;
        }
        .kpi-good .kpi-value {
            color: #15803d;
        }
        .kpi-medium {
            border-top-color: #f59e0b;
            background: #fffdf5;
        }
        .kpi-medium .kpi-value {
            color: #b45309;
        }
        .kpi-poor {
            border-top-color: #ef4444;
            background: #fffafa;
        }
        .kpi-poor .kpi-value {
            color: #dc2626;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# DATABASE CONNECTION

# ============================================================


def get_trino_connection():
    try:
        socket.gethostbyname(TRINO_HOST)
    except socket.gaierror as exc:
        raise ConnectionError(
            f"Cannot resolve Trino host '{TRINO_HOST}'. "
            "Ensure Streamlit and Trino are on the same Docker network."
        ) from exc
    try:
        connection = socket.create_connection(
            (TRINO_HOST, TRINO_PORT),
            timeout=5,
        )
        connection.close()
    except OSError as exc:
        raise ConnectionError(
            f"Trino is not reachable at " f"{TRINO_HOST}:{TRINO_PORT}."
        ) from exc
    kwargs = {
        "host": TRINO_HOST,
        "port": TRINO_PORT,
        "user": TRINO_USER,
        "catalog": TRINO_CATALOG,
        "schema": TRINO_SCHEMA,
    }
    if TRINO_PASSWORD:
        kwargs["auth"] = BasicAuthentication(
            TRINO_USER,
            TRINO_PASSWORD,
        )
    return connect(**kwargs)


# ============================================================
# LOAD DATA

# ============================================================


@st.cache_data(ttl=120)
def load_data():
    query = f"""
        SELECT
            primary_id,
            scan_id,
            data_level,
            definition_name,
            data_timestamp,
            scan_start_ts,
            scan_end_ts,
            has_errors,
            has_warnings,
            has_failures,
            table_name,
            row_count,
            source_data_inserted_at_utc,
            check_type,
            column_name,
            status,
            check_value,
            missing_count,
            invalid_count,
            max_column_timestamp,
            freshness,
            threshold,
            expected_type,
            actual_type
        FROM {FULL_TABLE_NAME}
        ORDER BY scan_start_ts DESC
    """
    connection = get_trino_connection()
    try:
        cursor = connection.cursor()
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [description[0] for description in cursor.description]
        return pd.DataFrame(
            rows,
            columns=columns,
        )
    finally:
        connection.close()


# ============================================================
# DATA PREPARATION

# ============================================================


def prepare_data(df):
    df = df.copy()
    timestamp_columns = [
        "data_timestamp",
        "scan_start_ts",
        "scan_end_ts",
        "source_data_inserted_at_utc",
        "max_column_timestamp",
    ]
    for column in timestamp_columns:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column],
                utc=True,
                errors="coerce",
            )
    string_columns = [
        "data_level",
        "definition_name",
        "table_name",
        "check_type",
        "column_name",
        "status",
        "freshness",
        "threshold",
        "expected_type",
        "actual_type",
    ]
    for column in string_columns:
        if column in df.columns:
            df[column] = df[column].fillna("").astype(str)
    df["status"] = df["status"].str.lower().str.strip()
    df["data_level"] = df["data_level"].str.lower().str.strip()
    df["quality_dimension"] = (
        df["check_type"].map(QUALITY_DIMENSION_MAP).fillna("Other")
    )
    df["scan_duration_seconds"] = (
        df["scan_end_ts"] - df["scan_start_ts"]
    ).dt.total_seconds()
    df["scan_date"] = df["scan_start_ts"].dt.floor("D")
    df["scan_day"] = df["scan_start_ts"].dt.strftime("%d %b %Y")
    df["check_label"] = (
        df["table_name"] + " · " + df["column_name"] + " · " + df["check_type"]
    )
    return df


# ============================================================
# FILTERING

# ============================================================


def apply_filters(
    df,
    data_levels,
    tables,
    check_types,
    statuses,
    start_date,
    end_date,
):
    filtered = df.copy()
    filtered = filtered[
        (filtered["scan_start_ts"].dt.date >= start_date)
        & (filtered["scan_start_ts"].dt.date <= end_date)
    ]
    if data_levels:
        filtered = filtered[filtered["data_level"].isin(data_levels)]
    if tables:
        filtered = filtered[filtered["table_name"].isin(tables)]
    if check_types:
        filtered = filtered[filtered["check_type"].isin(check_types)]
    if statuses:
        filtered = filtered[filtered["status"].isin(statuses)]
    return filtered


# ============================================================
# LATEST SCAN

# ============================================================


def get_latest_scan_rows(df):
    if df.empty:
        return df.copy()
    df = df.copy()
    df = df[df["scan_start_ts"].notna()].copy()
    if df.empty:
        return df
    latest_scan = df.groupby(
        [
            "data_level",
            "table_name",
        ],
        dropna=False,
    )[
        "scan_start_ts"
    ].transform("max")
    return df[df["scan_start_ts"].eq(latest_scan)].copy()


# ============================================================
# HELPERS

# ============================================================


def safe_percentage(
    numerator,
    denominator,
):
    if denominator == 0:
        return 0.0
    return numerator / denominator * 100


def section_header(
    title,
    description,
):
    st.markdown(
        f"""
        <div class="section-title">
            {title}
        </div>
        <div class="section-description">
            {description}
        </div>
        """,
        unsafe_allow_html=True,
    )


def style_figure(
    fig,
    height=380,
    show_legend=True,
):
    fig.update_layout(
        template="plotly_white",
        height=height,
        margin={
            "l": 20,
            "r": 20,
            "t": 85 if show_legend else 60,
            "b": 35,
        },
        font={
            "family": "Arial",
            "size": 12,
            "color": "#374151",
        },
        title={
            "x": 0.02,
            "xanchor": "left",
            "y": 0.97,
            "yanchor": "top",
            "font": {
                "size": 17,
                "color": "#30313d",
            },
        },
        legend={
            "orientation": "h",
            "yanchor": "top",
            "y": 1.10,
            "xanchor": "left",
            "x": 0.02,
            "title": {
                "text": "",
            },
            "font": {
                "size": 11,
            },
        },
        hoverlabel={
            "bgcolor": "#111827",
            "bordercolor": "#111827",
            "font": {
                "color": "#ffffff",
                "size": 12,
                "family": "Arial",
            },
            "namelength": -1,
        },
        showlegend=show_legend,
    )
    return fig


# ============================================================
# KPI CALCULATION

# ============================================================


def calculate_dashboard_kpis(df):
    total_checks = len(df)
    passed = int(df["status"].eq("pass").sum())
    failed = int(df["status"].eq("fail").sum())
    warnings = int(
        df["status"]
        .isin(
            [
                "warn",
                "warning",
            ]
        )
        .sum()
    )
    pass_rate = safe_percentage(
        passed,
        total_checks,
    )
    latest_scan = df["scan_start_ts"].max()
    scans = (
        df[
            [
                "scan_id",
                "scan_start_ts",
                "scan_end_ts",
            ]
        ]
        .drop_duplicates(subset=["scan_id"])
        .copy()
    )
    scans["duration_seconds"] = (
        scans["scan_end_ts"] - scans["scan_start_ts"]
    ).dt.total_seconds()
    avg_scan_time = scans["duration_seconds"].mean() if not scans.empty else 0
    return {
        "latest_scan": latest_scan,
        "tables": df["table_name"].nunique(),
        "scans": df["scan_id"].nunique(),
        "checks": total_checks,
        "passed": passed,
        "warnings": warnings,
        "failed": failed,
        "pass_rate": pass_rate,
        "avg_scan_time": avg_scan_time,
    }


# ============================================================
# TOP KPI CARDS

# ============================================================


def render_kpi_card(
    label,
    value,
    css_class="kpi-neutral",
    subtext=None,
):
    subtext_html = ""
    if subtext:
        subtext_html = f'<div class="kpi-subtext">{subtext}</div>'
    html = (
        f'<div class="kpi-card {css_class}">'
        f'<div class="kpi-label">{label}</div>'
        f'<div class="kpi-value">{value}</div>'
        f"{subtext_html}"
        f"</div>"
    )
    st.markdown(
        html,
        unsafe_allow_html=True,
    )


def get_quality_class(score):
    if score >= 90:
        return "kpi-good"
    if score >= 75:
        return "kpi-medium"
    return "kpi-poor"


def render_top_kpis(df):
    metrics = calculate_dashboard_kpis(df)
    latest_scan = (
        metrics["latest_scan"].strftime("%d %b %Y")
        if pd.notna(metrics["latest_scan"])
        else "N/A"
    )
    latest_scan_time = (
        metrics["latest_scan"].strftime("%H:%M UTC")
        if pd.notna(metrics["latest_scan"])
        else None
    )
    quality_class = get_quality_class(metrics["pass_rate"])
    # ========================================================
    # PRIMARY KPI ROW
    # ========================================================
    c1, c2, c3, c4, c5 = st.columns([1.35, 1, 1, 1.1, 1])
    with c1:
        render_kpi_card(
            label="Last Scan",
            value=latest_scan,
            css_class="kpi-neutral",
            subtext=latest_scan_time,
        )
    with c2:
        render_kpi_card(
            label="Tables Monitored",
            value=f"{metrics['tables']:,}",
            css_class="kpi-blue",
            subtext="Active datasets",
        )
    with c3:
        render_kpi_card(
            label="Scans",
            value=f"{metrics['scans']:,}",
            css_class="kpi-blue",
            subtext="Scan executions",
        )
    with c4:
        render_kpi_card(
            label="Checks Executed",
            value=f"{metrics['checks']:,}",
            css_class="kpi-blue",
            subtext="Quality checks",
        )
    with c5:
        render_kpi_card(
            label="Overall Quality",
            value=f"{metrics['pass_rate']:.1f}%",
            css_class=quality_class,
            subtext="Checks passed",
        )
    st.markdown(
        "<div style='height: 14px'></div>",
        unsafe_allow_html=True,
    )
    # ========================================================
    # STATUS KPI ROW
    # ========================================================
    c1, c2, c3, c4 = st.columns(4)
    with c1:
        render_kpi_card(
            label="Passed",
            value=f"{metrics['passed']:,}",
            css_class="kpi-success",
            subtext="Successful checks",
        )
    with c2:
        render_kpi_card(
            label="Warnings",
            value=f"{metrics['warnings']:,}",
            css_class="kpi-warning",
            subtext="Checks requiring review",
        )
    with c3:
        render_kpi_card(
            label="Failed",
            value=f"{metrics['failed']:,}",
            css_class="kpi-danger",
            subtext="Checks requiring action",
        )
    with c4:
        render_kpi_card(
            label="Avg. Scan Time",
            value=(
                f"{metrics['avg_scan_time']:.2f}s"
                if pd.notna(metrics["avg_scan_time"])
                else "N/A"
            ),
            css_class="kpi-violet",
            subtext="Average execution duration",
        )


# ============================================================
# STATUS DONUT

# ============================================================


def render_status_donut(df):
    status_summary = (
        df.groupby(
            "status",
            as_index=False,
        )
        .size()
        .rename(columns={"size": "checks"})
    )
    fig = px.pie(
        status_summary,
        names="status",
        values="checks",
        hole=0.62,
        color="status",
        color_discrete_map={
            "pass": "#22c55e",
            "warn": "#f59e0b",
            "warning": "#f59e0b",
            "fail": "#ef4444",
        },
    )
    fig.update_traces(
        textposition="outside",
        texttemplate="%{value} (%{percent})",
        textfont={
            "size": 12,
        },
        hovertemplate=(
            "<b>%{label}</b>"
            "<br>Checks: %{value}"
            "<br>Share: %{percent}"
            "<extra></extra>"
        ),
    )
    fig.update_layout(
        title="Check Status",
        uniformtext_minsize=10,
        uniformtext_mode="hide",
        margin={
            "l": 50,
            "r": 50,
            "t": 80,
            "b": 40,
        },
    )
    fig = style_figure(
        fig,
        height=400,
    )
    fig.update_layout(
        legend={
            "orientation": "h",
            "x": 0.5,
            "xanchor": "center",
            "y": -0.08,
            "yanchor": "top",
            "title": {
                "text": "",
            },
            "font": {
                "size": 11,
            },
        },
        margin={
            "l": 50,
            "r": 50,
            "t": 70,
            "b": 85,
        },
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# QUALITY BY DATA LAYER

# ============================================================


def render_layer_quality(df):
    layer_df = df.copy()
    layer_df["passed"] = layer_df["status"].eq("pass").astype(int)
    summary = layer_df.groupby(
        "data_level",
        as_index=False,
    ).agg(
        total_checks=(
            "primary_id",
            "count",
        ),
        passed_checks=(
            "passed",
            "sum",
        ),
    )
    summary["quality_score"] = summary["passed_checks"] / summary["total_checks"] * 100
    summary = summary.sort_values("quality_score")
    fig = px.bar(
        summary,
        x="quality_score",
        y="data_level",
        orientation="h",
        text="quality_score",
        custom_data=[
            "total_checks",
            "passed_checks",
        ],
    )
    fig.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b>"
            "<br>Quality score: <b>%{x:.1f}%</b>"
            "<br>Passed checks: %{customdata[1]:,.0f}"
            "<br>Total checks: %{customdata[0]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig.update_xaxes(
        range=[0, 105],
        title="Pass rate (%)",
    )
    fig.update_yaxes(
        title=None,
    )
    fig.update_layout(
        title="Quality by Data Layer",
    )
    fig = style_figure(
        fig,
        height=380,
        show_legend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# QUALITY DIMENSIONS

# ============================================================


def render_quality_dimensions(df):
    dimension_df = df[df["quality_dimension"] != "Other"].copy()
    if dimension_df.empty:
        st.info("No quality dimension data is available.")
        return
    dimension_df["passed"] = dimension_df["status"].eq("pass").astype(int)
    summary = dimension_df.groupby(
        "quality_dimension",
        as_index=False,
    ).agg(
        total_checks=(
            "primary_id",
            "count",
        ),
        passed_checks=(
            "passed",
            "sum",
        ),
    )
    summary["quality_score"] = summary["passed_checks"] / summary["total_checks"] * 100
    summary = summary.sort_values("quality_score")
    fig = px.bar(
        summary,
        x="quality_score",
        y="quality_dimension",
        orientation="h",
        text="quality_score",
        custom_data=[
            "total_checks",
            "passed_checks",
        ],
    )
    fig.update_traces(
        texttemplate="%{text:.1f}%",
        textposition="outside",
        hovertemplate=(
            "<b>%{y}</b>"
            "<br>Quality score: <b>%{x:.1f}%</b>"
            "<br>Passed checks: %{customdata[1]:,.0f}"
            "<br>Total checks: %{customdata[0]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig.update_xaxes(
        range=[0, 105],
        title="Pass rate (%)",
    )
    fig.update_yaxes(
        title=None,
    )
    fig.update_layout(
        title="Quality Dimensions",
    )
    fig = style_figure(
        fig,
        height=430,
        show_legend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# QUALITY TREND

# ============================================================


def render_quality_trend(df):
    trend = df.copy()
    trend["passed"] = trend["status"].eq("pass").astype(int)
    summary = trend.groupby(
        [
            "scan_date",
            "data_level",
        ],
        as_index=False,
    ).agg(
        total_checks=(
            "primary_id",
            "count",
        ),
        passed_checks=(
            "passed",
            "sum",
        ),
    )
    summary["pass_rate"] = summary["passed_checks"] / summary["total_checks"] * 100
    fig = px.line(
        summary,
        x="scan_date",
        y="pass_rate",
        color="data_level",
        markers=True,
        custom_data=[
            "total_checks",
            "passed_checks",
        ],
    )
    fig.update_traces(
        line={
            "width": 3,
        },
        marker={
            "size": 7,
        },
        hovertemplate=(
            "<b>%{fullData.name}</b>"
            "<br>Date: %{x|%d %b %Y}"
            "<br>Pass rate: <b>%{y:.1f}%</b>"
            "<br>Passed checks: %{customdata[1]:,.0f}"
            "<br>Total checks: %{customdata[0]:,.0f}"
            "<extra></extra>"
        ),
    )
    fig.update_yaxes(
        range=[0, 100],
        title="Pass rate (%)",
    )
    fig.update_xaxes(
        title=None,
    )
    fig.update_layout(
        title="Quality Pass Rate Over Time",
        hovermode="x unified",
        legend_title=None,
    )
    fig = style_figure(
        fig,
        height=410,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# STATUS OVER TIME

# ============================================================


def render_status_over_time(df):
    summary = (
        df.groupby(
            [
                "scan_date",
                "status",
            ],
            as_index=False,
        )
        .size()
        .rename(columns={"size": "checks"})
    )
    fig = px.bar(
        summary,
        x="scan_date",
        y="checks",
        color="status",
        barmode="stack",
        color_discrete_map={
            "pass": "#22c55e",
            "warn": "#f59e0b",
            "warning": "#f59e0b",
            "fail": "#ef4444",
        },
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{fullData.name}</b>"
            "<br>Date: %{x|%d %b %Y}"
            "<br>Checks: <b>%{y:,.0f}</b>"
            "<extra></extra>"
        ),
    )
    fig.update_xaxes(
        title=None,
    )
    fig.update_yaxes(
        title="Checks",
    )
    fig.update_layout(
        title="Check Outcomes Over Time",
        legend_title=None,
        hovermode="x unified",
    )
    fig = style_figure(
        fig,
        height=410,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# FAILING TABLES

# ============================================================


def render_failing_tables(df):
    failures = df[df["status"].eq("fail")].copy()
    if failures.empty:
        st.success("No failed checks for the selected period.")
        return
    summary = (
        failures.groupby(
            [
                "data_level",
                "table_name",
            ],
            as_index=False,
        )
        .size()
        .rename(columns={"size": "failures"})
        .sort_values(
            "failures",
            ascending=False,
        )
        .head(10)
    )
    fig = px.bar(
        summary.sort_values("failures"),
        x="failures",
        y="table_name",
        orientation="h",
        color="data_level",
        text="failures",
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{y}</b>"
            "<br>Layer: %{fullData.name}"
            "<br>Failed checks: <b>%{x:,.0f}</b>"
            "<extra></extra>"
        ),
    )
    fig.update_yaxes(
        title=None,
    )
    fig.update_xaxes(
        title="Failed checks",
    )
    fig.update_layout(
        title="Tables Requiring Attention",
        legend_title="Layer",
    )
    fig = style_figure(
        fig,
        height=420,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# FAILURE TYPES

# ============================================================


def render_failure_types(df):
    failures = df[df["status"].eq("fail")].copy()
    if failures.empty:
        st.success("No failed checks.")
        return
    summary = (
        failures.groupby(
            "check_type",
            as_index=False,
        )
        .size()
        .rename(columns={"size": "failures"})
        .sort_values("failures")
    )
    fig = px.bar(
        summary,
        x="failures",
        y="check_type",
        orientation="h",
        text="failures",
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{y}</b>" "<br>Failed checks: <b>%{x:,.0f}</b>" "<extra></extra>"
        ),
    )
    fig.update_yaxes(
        title=None,
    )
    fig.update_xaxes(
        title="Failed checks",
    )
    fig.update_layout(
        title="Failures by Check Type",
    )
    fig = style_figure(
        fig,
        height=420,
        show_legend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# SCAN DURATION TREND

# ============================================================


def render_scan_duration(df):
    scans = (
        df[
            [
                "scan_id",
                "scan_start_ts",
                "scan_end_ts",
            ]
        ]
        .drop_duplicates(subset=["scan_id"])
        .copy()
    )
    if scans.empty:
        st.info("No scan duration data is available.")
        return
    scans["duration_seconds"] = (
        scans["scan_end_ts"] - scans["scan_start_ts"]
    ).dt.total_seconds()
    scans["scan_date"] = scans["scan_start_ts"].dt.floor("D")
    summary = scans.groupby(
        "scan_date",
        as_index=False,
    )["duration_seconds"].mean()
    fig = px.line(
        summary,
        x="scan_date",
        y="duration_seconds",
        markers=True,
    )
    fig.update_traces(
        line={
            "width": 3,
        },
        marker={
            "size": 7,
        },
        hovertemplate=(
            "<b>Average Scan Duration</b>"
            "<br>Date: %{x|%d %b %Y}"
            "<br>Duration: <b>%{y:.2f} seconds</b>"
            "<extra></extra>"
        ),
    )
    fig.update_xaxes(
        title=None,
    )
    fig.update_yaxes(
        title="Seconds",
    )
    fig.update_layout(
        title="Average Scan Duration",
    )
    fig = style_figure(
        fig,
        height=370,
        show_legend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# CHECKS PER TABLE

# ============================================================


def render_checks_per_table(df):
    summary = (
        df.groupby(
            "table_name",
            as_index=False,
        )
        .size()
        .rename(columns={"size": "checks"})
        .sort_values("checks")
    )
    fig = px.bar(
        summary,
        x="checks",
        y="table_name",
        orientation="h",
        text="checks",
    )
    fig.update_traces(
        hovertemplate=(
            "<b>%{y}</b>" "<br>Checks executed: <b>%{x:,.0f}</b>" "<extra></extra>"
        ),
    )
    fig.update_yaxes(
        title=None,
    )
    fig.update_xaxes(
        title="Checks",
    )
    fig.update_layout(
        title="Checks by Table",
    )
    fig = style_figure(
        fig,
        height=370,
        show_legend=False,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# LATEST TABLE HEALTH

# ============================================================


def render_latest_table_health(df):
    latest = get_latest_scan_rows(df)
    if latest.empty:
        st.info("No latest scan data is available.")
        return
    latest = latest.copy()
    latest["passed_flag"] = latest["status"].eq("pass").astype(int)
    latest["failed_flag"] = latest["status"].eq("fail").astype(int)
    latest["warning_flag"] = (
        latest["status"]
        .isin(
            [
                "warn",
                "warning",
            ]
        )
        .astype(int)
    )
    summary = latest.groupby(
        [
            "data_level",
            "table_name",
        ],
        as_index=False,
    ).agg(
        row_count=(
            "row_count",
            "max",
        ),
        checks=(
            "primary_id",
            "count",
        ),
        passed=(
            "passed_flag",
            "sum",
        ),
        warnings=(
            "warning_flag",
            "sum",
        ),
        failed=(
            "failed_flag",
            "sum",
        ),
        last_scan=(
            "scan_start_ts",
            "max",
        ),
    )
    summary["quality_score"] = (summary["passed"] / summary["checks"] * 100).round(1)
    summary["health"] = summary.apply(
        lambda row: (
            "Critical"
            if row["failed"] > 0
            else ("Warning" if row["warnings"] > 0 else "Healthy")
        ),
        axis=1,
    )
    summary = summary[
        [
            "data_level",
            "table_name",
            "health",
            "row_count",
            "checks",
            "passed",
            "warnings",
            "failed",
            "quality_score",
            "last_scan",
        ]
    ]
    summary = summary.sort_values(
        [
            "quality_score",
            "failed",
        ],
        ascending=[
            True,
            False,
        ],
    )
    st.dataframe(
        summary,
        use_container_width=True,
        hide_index=True,
        column_config={
            "data_level": st.column_config.TextColumn(
                "Layer",
            ),
            "table_name": st.column_config.TextColumn(
                "Table",
            ),
            "health": st.column_config.TextColumn(
                "Health",
            ),
            "row_count": st.column_config.NumberColumn(
                "Rows",
                format="%d",
            ),
            "checks": st.column_config.NumberColumn(
                "Checks",
                format="%d",
            ),
            "passed": st.column_config.NumberColumn(
                "Passed",
                format="%d",
            ),
            "warnings": st.column_config.NumberColumn(
                "Warnings",
                format="%d",
            ),
            "failed": st.column_config.NumberColumn(
                "Failed",
                format="%d",
            ),
            "quality_score": st.column_config.ProgressColumn(
                "Quality",
                min_value=0,
                max_value=100,
                format="%.1f%%",
            ),
            "last_scan": st.column_config.DatetimeColumn(
                "Last Scan",
                format=("DD MMM YYYY HH:mm"),
            ),
        },
    )


# ============================================================
# STATUS HISTORY HEATMAP

# ============================================================


def render_status_heatmap(df):
    history = df.copy()
    history = history[history["scan_start_ts"].notna()].copy()
    if history.empty:
        st.info("No historical check results are available.")
        return
    history["scan_label"] = history["scan_start_ts"].dt.strftime("%d %b %H:%M")
    history["check_name"] = (
        history["table_name"]
        + " · "
        + history["column_name"]
        + " · "
        + history["check_type"]
    )
    history = history.sort_values("scan_start_ts").drop_duplicates(
        subset=[
            "scan_label",
            "check_name",
        ],
        keep="last",
    )
    problem_frequency = (
        history.assign(
            is_problem=(
                history["status"]
                .isin(
                    [
                        "fail",
                        "warn",
                        "warning",
                    ]
                )
                .astype(int)
            )
        )
        .groupby("check_name")["is_problem"]
        .sum()
        .sort_values(ascending=False)
    )
    if len(problem_frequency) > 30:
        selected_checks = problem_frequency.head(30).index
        history = history[history["check_name"].isin(selected_checks)]
    pivot = history.pivot_table(
        index="check_name",
        columns="scan_label",
        values="status",
        aggfunc="first",
    )
    if pivot.empty:
        st.info("No status history is available.")
        return
    status_map = {
        "fail": 0,
        "warn": 1,
        "warning": 1,
        "pass": 2,
    }
    numeric = pivot.replace(status_map)
    fig = go.Figure(
        data=go.Heatmap(
            z=numeric.values,
            x=numeric.columns.tolist(),
            y=numeric.index.tolist(),
            customdata=pivot.values,
            zmin=0,
            zmax=2,
            colorscale=[
                [
                    0.00,
                    "#ef4444",
                ],
                [
                    0.33,
                    "#ef4444",
                ],
                [
                    0.34,
                    "#f59e0b",
                ],
                [
                    0.66,
                    "#f59e0b",
                ],
                [
                    0.67,
                    "#22c55e",
                ],
                [
                    1.00,
                    "#22c55e",
                ],
            ],
            colorbar={
                "tickvals": [
                    0,
                    1,
                    2,
                ],
                "ticktext": [
                    "Fail",
                    "Warn",
                    "Pass",
                ],
                "title": "Status",
            },
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>Scan: %{x}"
                "<br>Status: <b>%{customdata}</b>"
                "<extra></extra>"
            ),
        )
    )
    fig.update_layout(
        title="Check Status History",
        template="plotly_white",
        height=max(
            430,
            len(pivot) * 26,
        ),
        margin={
            "l": 20,
            "r": 20,
            "t": 55,
            "b": 30,
        },
        xaxis_title=None,
        yaxis_title=None,
    )
    st.plotly_chart(
        fig,
        use_container_width=True,
        config={
            "displaylogo": False,
        },
    )


# ============================================================
# FAILURE DETAILS

# ============================================================


def render_failure_details(df):
    failures = df[df["status"].eq("fail")].copy()
    if failures.empty:
        st.success("No failed checks for the selected period.")
        return
    display_columns = [
        "data_level",
        "table_name",
        "column_name",
        "quality_dimension",
        "check_type",
        "check_value",
        "missing_count",
        "invalid_count",
        "expected_type",
        "actual_type",
        "freshness",
        "threshold",
        "scan_start_ts",
    ]
    display_columns = [
        column for column in display_columns if column in failures.columns
    ]
    st.dataframe(
        failures[display_columns].sort_values(
            "scan_start_ts",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# WARNING DETAILS

# ============================================================


def render_warning_details(df):
    warnings = df[
        df["status"].isin(
            [
                "warn",
                "warning",
            ]
        )
    ].copy()
    if warnings.empty:
        st.info("No warning checks for the selected period.")
        return
    display_columns = [
        "data_level",
        "table_name",
        "column_name",
        "quality_dimension",
        "check_type",
        "check_value",
        "missing_count",
        "invalid_count",
        "expected_type",
        "actual_type",
        "freshness",
        "threshold",
        "scan_start_ts",
    ]
    display_columns = [
        column for column in display_columns if column in warnings.columns
    ]
    st.dataframe(
        warnings[display_columns].sort_values(
            "scan_start_ts",
            ascending=False,
        ),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# SCHEMA DETAILS

# ============================================================


def render_schema_details(df):
    schema_df = df[df["check_type"].eq("schema")].copy()
    if schema_df.empty:
        st.info("No schema checks are available.")
        return
    total = len(schema_df)
    failed = int(schema_df["status"].eq("fail").sum())
    passed = int(schema_df["status"].eq("pass").sum())
    pass_rate = safe_percentage(
        passed,
        total,
    )
    c1, c2, c3 = st.columns(3)
    c1.metric(
        "Schema Checks",
        f"{total:,}",
        border=True,
    )
    c2.metric(
        "Schema Failures",
        f"{failed:,}",
        border=True,
    )
    c3.metric(
        "Schema Pass Rate",
        f"{pass_rate:.1f}%",
        border=True,
    )
    st.dataframe(
        schema_df[
            [
                "data_level",
                "table_name",
                "column_name",
                "status",
                "expected_type",
                "actual_type",
                "scan_start_ts",
            ]
        ].sort_values(
            [
                "status",
                "table_name",
                "column_name",
            ]
        ),
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# RAW CHECK RESULTS

# ============================================================


def render_all_results(df):
    display_columns = [
        "scan_id",
        "data_level",
        "definition_name",
        "table_name",
        "column_name",
        "quality_dimension",
        "check_type",
        "status",
        "check_value",
        "missing_count",
        "invalid_count",
        "freshness",
        "threshold",
        "expected_type",
        "actual_type",
        "row_count",
        "scan_start_ts",
        "scan_end_ts",
    ]
    display_columns = [column for column in display_columns if column in df.columns]
    results_df = (
        df[display_columns]
        .sort_values(
            "scan_start_ts",
            ascending=False,
        )
        .reset_index(drop=True)
    )
    if results_df.empty:
        st.info("No check results for the selected period.")
        return
    page_size = st.selectbox(
        "Rows per page",
        options=[10, 25, 50, 100],
        index=1,
        key="all_check_results_page_size",
    )
    total_rows = len(results_df)
    total_pages = max(
        1,
        (total_rows + page_size - 1) // page_size,
    )
    page_key = "all_check_results_page"
    if page_key not in st.session_state:
        st.session_state[page_key] = 1
    if st.session_state[page_key] > total_pages:
        st.session_state[page_key] = total_pages
    if st.session_state[page_key] < 1:
        st.session_state[page_key] = 1
    previous_col, page_col, next_col = st.columns([1, 2, 1])
    with previous_col:
        if st.button(
            "Previous",
            use_container_width=True,
            disabled=(st.session_state[page_key] <= 1),
            key="all_check_results_previous",
        ):
            st.session_state[page_key] -= 1
            st.rerun()
    with page_col:
        start_record = (st.session_state[page_key] - 1) * page_size + 1
        end_record = min(
            st.session_state[page_key] * page_size,
            total_rows,
        )
        st.markdown(
            (
                "<div style='text-align:center;"
                "color:#6b7280;font-size:0.9rem;"
                "padding-top:0.45rem;'>"
                f"Page <strong>{st.session_state[page_key]}</strong> "
                f"of <strong>{total_pages}</strong> · "
                f"Showing <strong>{start_record:,}</strong>–"
                f"<strong>{end_record:,}</strong> of "
                f"<strong>{total_rows:,}</strong> results"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    with next_col:
        if st.button(
            "Next",
            use_container_width=True,
            disabled=(st.session_state[page_key] >= total_pages),
            key="all_check_results_next",
        ):
            st.session_state[page_key] += 1
            st.rerun()
    start_row = (st.session_state[page_key] - 1) * page_size
    end_row = min(
        start_row + page_size,
        total_rows,
    )
    page_df = results_df.iloc[start_row:end_row]
    st.dataframe(
        page_df,
        use_container_width=True,
        hide_index=True,
    )


# ============================================================
# SIDEBAR FILTERS

# ============================================================


def render_filters(df):
    st.sidebar.markdown("## Data Quality Filters")
    levels = sorted(value for value in df["data_level"].unique() if value)
    selected_levels = st.sidebar.multiselect(
        "Data Level",
        options=levels,
        default=levels,
    )
    available_tables = (
        df[df["data_level"].isin(selected_levels)]["table_name"]
        if selected_levels
        else df["table_name"]
    )
    tables = sorted(value for value in available_tables.unique() if value)
    selected_tables = st.sidebar.multiselect(
        "Table",
        options=tables,
    )
    check_types = sorted(value for value in df["check_type"].unique() if value)
    selected_check_types = st.sidebar.multiselect(
        "Check Type",
        options=check_types,
    )
    statuses = sorted(value for value in df["status"].unique() if value)
    selected_statuses = st.sidebar.multiselect(
        "Status",
        options=statuses,
    )
    min_timestamp = df["scan_start_ts"].min()
    max_timestamp = df["scan_start_ts"].max()
    if pd.isna(min_timestamp) or pd.isna(max_timestamp):
        raise ValueError("No valid scan_start_ts values " "were found.")
    min_date = min_timestamp.date()
    max_date = max_timestamp.date()
    period = st.sidebar.selectbox(
        "Monitoring Period",
        [
            "7 Days",
            "30 Days",
            "60 Days",
            "90 Days",
            "Custom",
        ],
        index=1,
    )
    period_map = {
        "7 Days": 7,
        "30 Days": 30,
        "60 Days": 60,
        "90 Days": 90,
    }
    if period in period_map:
        end_date = max_date
        start_date = max(
            min_date,
            end_date - timedelta(days=(period_map[period] - 1)),
        )
    else:
        start_date = st.sidebar.date_input(
            "Start Date",
            value=min_date,
            min_value=min_date,
            max_value=max_date,
        )
        end_date = st.sidebar.date_input(
            "End Date",
            value=max_date,
            min_value=min_date,
            max_value=max_date,
        )
    if start_date > end_date:
        st.sidebar.error("Start date must be before end date.")
        st.stop()
    return {
        "levels": selected_levels,
        "tables": selected_tables,
        "check_types": selected_check_types,
        "statuses": selected_statuses,
        "start_date": start_date,
        "end_date": end_date,
    }


# ============================================================
# MAIN

# ============================================================


def main():
    st.markdown(
        """
        <div class="dq-title">
            Data Quality Monitoring
        </div>
        """,
        unsafe_allow_html=True,
    )
    st.markdown(
        f"""
        <div class="dq-subtitle">
            Soda quality monitoring across
            Bronze, Silver and Gold data layers.
            Source:
            <strong>
                {FULL_TABLE_NAME}
            </strong>
        </div>
        """,
        unsafe_allow_html=True,
    )
    try:
        df = load_data()
    except ConnectionError as exc:
        st.error("Unable to connect to Trino.")
        st.markdown(f"""
            **Trino configuration**
            - Host: `{TRINO_HOST}`
            - Port: `{TRINO_PORT}`
            - Catalog: `{TRINO_CATALOG}`
            - Schema: `{TRINO_SCHEMA}`
            - Table: `{TRINO_DQ_TABLE}`
            """)
        with st.expander("Technical details"):
            st.exception(exc)
        return
    except Exception as exc:
        st.error("Unable to load data quality results.")
        with st.expander("Technical details"):
            st.exception(exc)
        return
    if df.empty:
        st.warning("No data quality results are currently available.")
        return
    df = prepare_data(df)
    filters = render_filters(df)
    filtered_df = apply_filters(
        df=df,
        data_levels=filters["levels"],
        tables=filters["tables"],
        check_types=filters["check_types"],
        statuses=filters["statuses"],
        start_date=filters["start_date"],
        end_date=filters["end_date"],
    )
    st.markdown(
        f"""
        <div class="dq-period">
            <strong>
                Monitoring period:
            </strong>
            {filters['start_date']}
            to
            {filters['end_date']}
        </div>
        """,
        unsafe_allow_html=True,
    )
    if filtered_df.empty:
        st.warning("No records match the selected filters.")
        return
    # ========================================================
    # KPIs
    # ========================================================
    render_top_kpis(filtered_df)
    st.divider()
    # ========================================================
    # HEALTH OVERVIEW
    # ========================================================
    section_header(
        "Quality Health",
        (
            "Understand the current distribution "
            "of quality outcomes and compare "
            "health across data layers."
        ),
    )
    c1, c2 = st.columns(
        [
            1,
            1.4,
        ]
    )
    with c1:
        render_status_donut(filtered_df)
    with c2:
        render_layer_quality(filtered_df)
    render_quality_dimensions(filtered_df)
    st.divider()
    # ========================================================
    # TREND
    # ========================================================
    section_header(
        "Quality Trend",
        ("Track whether data quality is " "improving or deteriorating over time."),
    )
    c1, c2 = st.columns(2)
    with c1:
        render_quality_trend(filtered_df)
    with c2:
        render_status_over_time(filtered_df)
    c1, c2 = st.columns(2)
    with c1:
        render_scan_duration(filtered_df)
    with c2:
        render_checks_per_table(filtered_df)
    st.divider()
    # ========================================================
    # PROBLEM AREAS
    # ========================================================
    section_header(
        "Problem Areas",
        ("Identify tables and check types " "that require the most attention."),
    )
    c1, c2 = st.columns(2)
    with c1:
        render_failing_tables(filtered_df)
    with c2:
        render_failure_types(filtered_df)
    st.divider()
    # ========================================================
    # TABLE HEALTH
    # ========================================================
    section_header(
        "Latest Table Health",
        ("Review the most recent quality " "position of every monitored table."),
    )
    render_latest_table_health(filtered_df)
    st.divider()
    # ========================================================
    # STATUS HISTORY
    # ========================================================
    section_header(
        "Check Status History",
        (
            "Identify persistent, intermittent "
            "and newly occurring data quality "
            "problems."
        ),
    )
    selected_tables = filters["tables"]
    if len(selected_tables) == 1:
        render_status_heatmap(filtered_df)
    elif len(selected_tables) > 1:
        st.info(
            "Please select only one table in the sidebar to view "
            "Check Status History."
        )
    else:
        st.info("Apply a table filter in the sidebar to view " "Check Status History.")
    st.divider()
    # ========================================================
    # DIAGNOSTICS
    # ========================================================
    section_header(
        "Quality Diagnostics",
        ("Drill into failures, warnings, " "schema differences and individual checks."),
    )
    (
        failures_tab,
        warnings_tab,
        schema_tab,
        results_tab,
    ) = st.tabs(
        [
            "Failed Checks",
            "Warning Checks",
            "Schema",
            "All Check Results",
        ]
    )
    with failures_tab:
        render_failure_details(filtered_df)
    with warnings_tab:
        render_warning_details(filtered_df)
    with schema_tab:
        render_schema_details(filtered_df)
    with results_tab:
        render_all_results(filtered_df)
    # ========================================================
    # REFRESH
    # ========================================================
    st.sidebar.divider()
    if st.sidebar.button(
        "Refresh Data",
        use_container_width=True,
    ):
        st.cache_data.clear()
        st.rerun()


main()
