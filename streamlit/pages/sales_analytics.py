import os
import socket
from datetime import timedelta

import pandas as pd
import plotly.express as px
from trino.auth import BasicAuthentication

import streamlit as st
import trino

# ============================================================
# PAGE CONFIG

# ============================================================
# st.set_page_config(
#     page_title="Sales Analytics",
#     page_icon="📊",
#     layout="wide",
#     initial_sidebar_state="expanded",
# )

# ============================================================
# CUSTOM STYLING

# ============================================================
st.markdown(
    """
    <style>
        .stApp {
            background: #ffffff;
            color: #30313d;
        }
        .main .block-container {
            max-width: 1500px;
            padding: 2rem 3rem 3rem 3rem;
        }
        h1, h2, h3 {
            color: #30313d !important;
            font-weight: 750 !important;
            letter-spacing: -0.02em;
        }
        h1 {
            font-size: 2.65rem !important;
            margin-bottom: 0.15rem !important;
        }
        h2 {
            font-size: 1.65rem !important;
        }
        h3 {
            font-size: 1.15rem !important;
        }
        p, label, .stMarkdown {
            color: #5f606b;
        }
        section[data-testid="stSidebar"] {
            background: #292b35 !important;
            border-right: 1px solid #20222b;
        }
        section[data-testid="stSidebar"] > div {
            background: #292b35 !important;
        }
        section[data-testid="stSidebar"] * {
            color: #f4f5f7 !important;
        }
        section[data-testid="stSidebar"] h2 {
            font-size: 1.05rem !important;
            margin-top: 0.5rem !important;
        }
        section[data-testid="stSidebar"] .stSelectbox > div > div,
        section[data-testid="stSidebar"] .stMultiSelect > div > div,
        section[data-testid="stSidebar"] .stDateInput > div > div {
            background: #11141b !important;
            border: 1px solid #363946 !important;
            border-radius: 9px !important;
        }
        section[data-testid="stSidebar"] input {
            color: #ffffff !important;
            background: #11141b !important;
        }
        section[data-testid="stSidebar"] [data-baseweb="select"] {
            background: #11141b !important;
        }
        section[data-testid="stSidebar"] [data-baseweb="tag"] {
            background: #ff5c61 !important;
            border-radius: 7px !important;
        }
        .dashboard-title {
            display: flex;
            align-items: center;
            gap: 14px;
            margin-bottom: 4px;
        }
        .dashboard-icon {
            width: 38px;
            height: 38px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 23px;
            font-weight: 800;
            color: #30313d;
        }
        .source-text {
            color: #777985;
            font-size: 0.95rem;
            margin-bottom: 0.25rem;
        }
        .analysis-period {
            color: #30313d;
            font-size: 0.95rem;
            margin-bottom: 0.4rem;
        }
        .kpi-period {
            color: #777985;
            font-size: 0.85rem;
            margin-bottom: 1.6rem;
        }
        div[data-testid="stMetric"] {
            background: #ffffff;
            border-radius: 14px;
        }
        .section-header {
            display: flex;
            align-items: center;
            gap: 10px;
            margin-top: 2rem;
            margin-bottom: 0.15rem;
        }
        .section-icon {
            font-size: 1.45rem;
        }
        .section-title {
            color: #30313d;
            font-size: 1.55rem;
            font-weight: 750;
        }
        .section-description {
            color: #888995;
            font-size: 0.9rem;
            margin-bottom: 1rem;
        }
        .chart-title {
            color: #30313d;
            font-size: 1.05rem;
            font-weight: 700;
            margin-bottom: 0.25rem;
        }
        .stPlotlyChart {
            border: 1px solid #e5e6eb;
            border-radius: 13px;
            padding: 4px;
            background: #ffffff;
        }
        hr {
            border: none !important;
            border-top: 1px solid #e4e5e9 !important;
            margin: 1.25rem 0 !important;
        }
        [data-testid="stDataFrame"] {
            border: 1px solid #e1e2e7;
            border-radius: 12px;
            overflow: hidden;
            box-shadow: 0 1px 2px rgba(17, 24, 39, 0.04);
        }
        [data-testid="stDataFrame"] [role="columnheader"] {
            font-weight: 700 !important;
            color: #374151 !important;
        }
        .customer-table-meta {
            color: #6b7280;
            font-size: 0.88rem;
            margin: 0.2rem 0 0.7rem 0;
        }
        .customer-table-meta strong {
            color: #374151;
            font-weight: 700;
        }
        #MainMenu {
            visibility: hidden;
        }
        footer {
            visibility: hidden;
        }
    </style>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# TRINO CONFIGURATION

# ============================================================
TRINO_HOST = os.getenv("TRINO_HOST", "trino")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8089"))
TRINO_USER = os.getenv("TRINO_USER", "airflow")
TRINO_PASSWORD = os.getenv("TRINO_PASSWORD")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "iceberg")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "gold")
SALES_AGG_TABLE = os.getenv(
    "SALES_AGG_TABLE",
    "agg_daily_sales",
)
CUSTOMER_LTV_TABLE = os.getenv(
    "CUSTOMER_LTV_TABLE",
    "agg_customer_lifetime_value",
)


def get_connection():
    """Create a Trino connection."""
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
            f"Trino is not reachable at {TRINO_HOST}:{TRINO_PORT}."
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
    return trino.dbapi.connect(**kwargs)


@st.cache_data(ttl=300)
def load_df(query: str) -> pd.DataFrame:
    """Execute a Trino query and cache the result for five minutes."""
    conn = get_connection()
    try:
        return pd.read_sql(query, conn)
    finally:
        conn.close()


@st.cache_data(ttl=300)
def load_filter_values():
    """Load dashboard filter values from the daily Gold aggregate."""
    stores = load_df(f"""
        SELECT DISTINCT store_name
        FROM {SALES_AGG_TABLE}
        WHERE store_name IS NOT NULL
        ORDER BY store_name
        """)["store_name"].tolist()
    categories = load_df(f"""
        SELECT DISTINCT category
        FROM {SALES_AGG_TABLE}
        WHERE category IS NOT NULL
        ORDER BY category
        """)["category"].tolist()
    regions = load_df(f"""
        SELECT DISTINCT region
        FROM {SALES_AGG_TABLE}
        WHERE region IS NOT NULL
        ORDER BY region
        """)["region"].tolist()
    return stores, categories, regions


@st.cache_data(ttl=300)
def load_countries(selected_regions: tuple[str, ...] = ()) -> list[str]:
    """Load countries, filtered by the selected region(s) when supplied."""
    where_clause = "country IS NOT NULL"
    if selected_regions:
        where_clause += f" AND region IN ({sql_string_list(list(selected_regions))})"
    return load_df(f"""
        SELECT DISTINCT country
        FROM {SALES_AGG_TABLE}
        WHERE {where_clause}
        ORDER BY country
        """)["country"].tolist()


@st.cache_data(ttl=300)
def get_max_transaction_date():
    """
    Get the latest sales date available in agg_daily_sales.
    This is used as the dashboard's effective current date instead
    of Python's date.today().
    """
    result = load_df(f"""
        SELECT MAX(sales_date) AS max_date
        FROM {SALES_AGG_TABLE}
        """)
    max_date = result.iloc[0]["max_date"]
    if pd.isna(max_date):
        raise ValueError(f"{SALES_AGG_TABLE} contains no sales dates.")
    return pd.Timestamp(max_date).date()


def sql_string_list(values: list[str]) -> str:
    """Safely format strings for SQL IN clauses."""
    return ", ".join("'" + str(value).replace("'", "''") + "'" for value in values)


def format_number(value) -> str:
    """Format numeric values safely."""
    if pd.isna(value):
        return "0"
    return f"{float(value):,.0f}"


def format_currency(value) -> str:
    """Format currency values."""
    if pd.isna(value):
        return "0.00"
    return f"{float(value):,.2f}"


def section_header(
    icon: str,
    title: str,
    description: str,
):
    """Render a dashboard section header."""
    st.markdown(
        f"""
        <div class="section-header">
            <span class="section-icon">{icon}</span>
            <span class="section-title">{title}</span>
        </div>
        <div class="section-description">
            {description}
        </div>
        """,
        unsafe_allow_html=True,
    )


def get_delta(
    current: float,
    previous: float,
) -> str | None:
    """
    Calculate percentage change versus the previous period.
    Returns None when there is no meaningful previous-period
    denominator.
    """
    if previous is None or pd.isna(previous):
        return None
    if previous == 0:
        return None
    change = ((current - previous) / previous) * 100
    return f"{change:+.1f}%"


def chart_values(values) -> list[float]:
    """Convert a trend series to values accepted by st.metric."""
    if values is None or len(values) == 0:
        return [0.0]
    return [0.0 if pd.isna(value) else float(value) for value in values]


def style_chart(fig, height=420, show_legend=False):
    """Apply consistent dashboard styling to Plotly charts."""
    fig.update_layout(
        height=height,
        template="plotly_white",
        paper_bgcolor="#ffffff",
        plot_bgcolor="#ffffff",
        font={
            "family": "Arial, sans-serif",
            "size": 12,
            "color": "#4b5563",
        },
        margin={
            "l": 35,
            "r": 25,
            "t": 25,
            "b": 45,
        },
        hoverlabel={
            "bgcolor": "#111827",
            "bordercolor": "#111827",
            "font": {
                "color": "#ffffff",
                "size": 12,
                "family": "Arial, sans-serif",
            },
            "namelength": -1,
        },
        showlegend=show_legend,
    )
    fig.update_xaxes(
        showgrid=False,
        zeroline=False,
        linecolor="#e5e7eb",
        tickcolor="#e5e7eb",
        tickfont={"color": "#6b7280"},
        title_font={"color": "#374151"},
    )
    fig.update_yaxes(
        showgrid=True,
        gridcolor="#eef0f3",
        gridwidth=1,
        zeroline=False,
        linecolor="#e5e7eb",
        tickcolor="#e5e7eb",
        tickfont={"color": "#6b7280"},
        title_font={"color": "#374151"},
    )
    return fig


def style_currency_axis(fig, axis="y"):
    """Format a Plotly axis as compact currency."""
    if axis == "x":
        fig.update_xaxes(tickprefix="$", tickformat="~s")
    else:
        fig.update_yaxes(tickprefix="$", tickformat="~s")
    return fig


# ============================================================
# LOAD FILTER VALUES / CONNECTION CHECK

# ============================================================
try:
    stores, categories, regions = load_filter_values()
    max_transaction_date = get_max_transaction_date()
except ConnectionError as exc:
    st.error("Unable to connect to Trino.")
    st.markdown(f"""
        **Trino configuration**
        - Host: `{TRINO_HOST}`
        - Port: `{TRINO_PORT}`
        - Catalog: `{TRINO_CATALOG}`
        - Schema: `{TRINO_SCHEMA}`
        """)
    st.warning(
        "If Streamlit is running in Docker, make sure it is "
        "attached to the same Docker network as Trino."
    )
    with st.expander("Technical details"):
        st.exception(exc)
    st.stop()
except Exception as exc:
    st.error("The dashboard could not load the data.")
    with st.expander("Technical details"):
        st.exception(exc)
    st.stop()

# ============================================================
# SIDEBAR

# ============================================================
st.sidebar.markdown(
    """
    <div style="
        font-size: 1.15rem;
        font-weight: 750;
        margin: 0.5rem 0 1.3rem 0;
    ">
        ◈ Filters
    </div>
    """,
    unsafe_allow_html=True,
)
if st.sidebar.button(
    "Refresh data",
    use_container_width=True,
):
    st.cache_data.clear()
    st.rerun()
reporting_period = st.sidebar.selectbox(
    "Reporting period",
    [
        "7 Days",
        "30 Days",
        "60 Days",
        "90 Days",
        "12 Months",
        "Custom",
    ],
    index=1,
)

# ============================================================
# REPORTING PERIOD

# ============================================================
period_days_map = {
    "7 Days": 7,
    "30 Days": 30,
    "60 Days": 60,
    "90 Days": 90,
    "12 Months": 365,
}
if reporting_period in period_days_map:
    period_days = period_days_map[reporting_period]
    end_date = max_transaction_date
    start_date = end_date - timedelta(days=period_days - 1)
else:
    start_date = st.sidebar.date_input(
        "Start date",
        value=max_transaction_date - timedelta(days=59),
        max_value=max_transaction_date,
    )
    end_date = st.sidebar.date_input(
        "End date",
        value=max_transaction_date,
        max_value=max_transaction_date,
    )
if start_date > end_date:
    st.sidebar.error("Start date must be before end date.")
    st.stop()

selected_regions = st.sidebar.multiselect(
    "Region",
    regions,
)

countries = load_countries(tuple(selected_regions))

country_filter_key = "sales_country_filter"

# Remove previously selected countries that are no longer valid
# after the Region selection changes.
if country_filter_key in st.session_state:
    st.session_state[country_filter_key] = [
        country
        for country in st.session_state[country_filter_key]
        if country in countries
    ]

selected_countries = st.sidebar.multiselect(
    "Country",
    countries,
    key=country_filter_key,
)
selected_stores = st.sidebar.multiselect(
    "Store",
    stores,
)
selected_categories = st.sidebar.multiselect(
    "Category",
    categories,
)
statuses = st.sidebar.multiselect(
    "Status",
    [
        "completed",
        "pending",
        "cancelled",
    ],
    default=["completed"],
)

# ============================================================
# CURRENT / PREVIOUS PERIOD FILTERS

# ============================================================
# Main dashboard filter for agg_daily_sales
where = [f"""
    sales_date >= DATE '{start_date}'
    AND sales_date <= DATE '{end_date}'
    """]
if selected_stores:
    where.append(f"store_name IN ({sql_string_list(selected_stores)})")
if selected_categories:
    where.append(f"category IN ({sql_string_list(selected_categories)})")
if selected_regions:
    where.append(f"region IN ({sql_string_list(selected_regions)})")
if selected_countries:
    where.append(f"country IN ({sql_string_list(selected_countries)})")
if statuses:
    where.append(f"sales_status IN ({sql_string_list(statuses)})")
sales_filter = " AND ".join(where)

# ============================================================
# KPI COMPARISON PERIOD

# ============================================================
kpi_period_days = (end_date - start_date).days + 1
kpi_current_start = start_date
kpi_current_end = end_date
kpi_previous_end = kpi_current_start - timedelta(days=1)
kpi_previous_start = kpi_previous_end - timedelta(days=kpi_period_days - 1)

# ============================================================
# PREVIOUS AGGREGATE FILTER

# ============================================================
previous_where = [f"""
    sales_date >= DATE '{kpi_previous_start}'
    AND sales_date <= DATE '{kpi_previous_end}'
    """]
if selected_stores:
    previous_where.append(f"store_name IN ({sql_string_list(selected_stores)})")
if selected_categories:
    previous_where.append(f"category IN ({sql_string_list(selected_categories)})")
if selected_regions:
    previous_where.append(f"region IN ({sql_string_list(selected_regions)})")
if selected_countries:
    previous_where.append(f"country IN ({sql_string_list(selected_countries)})")
if statuses:
    previous_where.append(f"sales_status IN ({sql_string_list(statuses)})")
previous_filter = " AND ".join(previous_where)

# ============================================================
# PAGE HEADER

# ============================================================
st.markdown(
    """
    <div class="dashboard-title">
        <div class="dashboard-icon">▮▮</div>
        <h1>Sales Analytics Dashboard</h1>
    </div>
    """,
    unsafe_allow_html=True,
)
st.markdown(
    f"""
    <div class="source-text">
        Source:
        <strong>
            Trino / {TRINO_CATALOG}.{TRINO_SCHEMA}.{SALES_AGG_TABLE}
        </strong>
    </div>
    <div class="analysis-period">
        <strong>Analysis period:</strong>
        {start_date} to {end_date}
    </div>
    <div class="kpi-period">
        <strong>KPI comparison:</strong>
        {kpi_current_start} to {kpi_current_end}
         vs
        {kpi_previous_start} to {kpi_previous_end}
    </div>
    """,
    unsafe_allow_html=True,
)

# ============================================================
# CURRENT DAILY TREND

# ============================================================
daily = load_df(f"""
    SELECT
        sales_date,
        COALESCE(SUM(total_revenue), 0) AS revenue,
        COALESCE(SUM(total_transactions), 0) AS orders,
        COALESCE(SUM(total_units_sold), 0) AS units,
        COALESCE(
            SUM(total_revenue)
            / NULLIF(SUM(total_transactions), 0),
            0
        ) AS average_order_value
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY sales_date
    ORDER BY sales_date
    """)

country_sales = load_df(f"""
    SELECT
        country,
        COALESCE(SUM(total_transactions), 0) AS orders,
        COALESCE(SUM(total_revenue), 0) AS revenue,
        COALESCE(SUM(total_units_sold), 0) AS units
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY country
    ORDER BY orders DESC
    """)

# ============================================================
# CURRENT KPI VALUES

# ============================================================
current_kpis = load_df(f"""
    SELECT
        COALESCE(SUM(total_revenue), 0) AS total_revenue,
        COALESCE(SUM(total_transactions), 0) AS total_orders,
        COALESCE(SUM(total_units_sold), 0) AS units_sold,
        COALESCE(
            SUM(total_revenue)
            / NULLIF(SUM(total_transactions), 0),
            0
        ) AS avg_order_value
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    """).iloc[0]

# ============================================================
# PREVIOUS KPI VALUES

# ============================================================
previous_kpis = load_df(f"""
    SELECT
        COALESCE(SUM(total_revenue), 0) AS total_revenue,
        COALESCE(SUM(total_transactions), 0) AS total_orders,
        COALESCE(SUM(total_units_sold), 0) AS units_sold,
        COALESCE(
            SUM(total_revenue)
            / NULLIF(SUM(total_transactions), 0),
            0
        ) AS avg_order_value
    FROM {SALES_AGG_TABLE}
    WHERE {previous_filter}
    """).iloc[0]

# ============================================================
# KPI VALUES

# ============================================================
current_revenue = float(current_kpis["total_revenue"] or 0)
current_orders = float(current_kpis["total_orders"] or 0)
current_units = float(current_kpis["units_sold"] or 0)
current_aov = float(current_kpis["avg_order_value"] or 0)
previous_revenue = float(previous_kpis["total_revenue"] or 0)
previous_orders = float(previous_kpis["total_orders"] or 0)
previous_units = float(previous_kpis["units_sold"] or 0)
previous_aov = float(previous_kpis["avg_order_value"] or 0)

# ============================================================
# KPI DELTAS

# ============================================================
revenue_delta = get_delta(
    current_revenue,
    previous_revenue,
)
orders_delta = get_delta(
    current_orders,
    previous_orders,
)
units_delta = get_delta(
    current_units,
    previous_units,
)
aov_delta = get_delta(
    current_aov,
    previous_aov,
)

# ============================================================
# SPARKLINE DATA

# ============================================================
spark_revenue = chart_values(daily["revenue"].tolist() if not daily.empty else [])
spark_orders = chart_values(daily["orders"].tolist() if not daily.empty else [])
spark_units = chart_values(daily["units"].tolist() if not daily.empty else [])
spark_aov = chart_values(
    daily["average_order_value"].tolist() if not daily.empty else []
)

# ============================================================
# KPI CARDS

# ============================================================
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(
        "Total Revenue",
        f"${current_revenue:,.0f}",
        delta=revenue_delta,
        border=True,
        chart_data=spark_revenue,
        chart_type="area",
    )
with col2:
    st.metric(
        "Total Orders",
        f"{current_orders:,.0f}",
        delta=orders_delta,
        border=True,
        chart_data=spark_orders,
        chart_type="bar",
    )
with col3:
    st.metric(
        "Units Sold",
        f"{current_units:,.0f}",
        delta=units_delta,
        border=True,
        chart_data=spark_units,
        chart_type="bar",
    )
with col4:
    st.metric(
        "Avg. Order Value",
        f"${current_aov:,.0f}",
        delta=aov_delta,
        border=True,
        chart_data=spark_aov,
        chart_type="line",
    )

# ============================================================
# TREND SECTION

# ============================================================
section_header(
    "↗",
    "How are we trending?",
    "Track revenue and orders over time and identify changes in sales activity.",
)
c1, c2 = st.columns(2)
with c1:
    st.markdown(
        '<div class="chart-title">Revenue trend</div>',
        unsafe_allow_html=True,
    )
    if daily.empty:
        st.info("No revenue data is available for the selected filters.")
    else:
        fig = px.line(
            daily,
            x="sales_date",
            y="revenue",
            markers=True,
            custom_data=[
                "orders",
                "units",
                "average_order_value",
            ],
        )
        fig.update_traces(
            line={"width": 3, "color": "#4f46e5"},
            marker={
                "size": 7,
                "color": "#4f46e5",
                "line": {"width": 2, "color": "#ffffff"},
            },
            hovertemplate=(
                "<b>%{x|%d %b %Y}</b>"
                "<br>Revenue: <b>$%{y:,.2f}</b>"
                "<br>Orders: %{customdata[0]:,.0f}"
                "<br>Units sold: %{customdata[1]:,.0f}"
                "<br>Avg. order value: $%{customdata[2]:,.2f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=390)
        fig = style_currency_axis(fig, axis="y")
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Revenue",
            hovermode="closest",
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )
with c2:
    st.markdown(
        '<div class="chart-title">Revenue by country</div>',
        unsafe_allow_html=True,
    )
    if country_sales.empty:
        st.info("No country sales data is available for the selected filters.")
    else:
        fig = px.bar(
            country_sales.sort_values("revenue"),
            x="revenue",
            y="country",
            orientation="h",
            custom_data=[
                "orders",
                "units",
            ],
        )
        fig.update_traces(
            marker={
                "color": "#6366f1",
                "line": {"width": 0},
            },
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>Revenue: <b>$%{x:,.2f}</b>"
                "<br>Orders: %{customdata[0]:,.0f}"
                "<br>Units sold: %{customdata[1]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=390)
        fig.update_layout(
            xaxis_title="Revenue",
            yaxis_title=None,
            hovermode="closest",
            bargap=0.20,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )

# ============================================================
# PRODUCT / CATEGORY PERFORMANCE

# ============================================================
section_header(
    "↗",
    "What is driving sales?",
    "Compare product and category performance within the selected reporting period.",
)
product = load_df(f"""
    SELECT
        product_name,
        category,
        COALESCE(SUM(total_units_sold), 0) AS units_sold,
        COALESCE(SUM(total_revenue), 0) AS revenue,
        COALESCE(SUM(total_transactions), 0) AS orders
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY
        product_name,
        category
    ORDER BY revenue DESC
    LIMIT 15
    """)
category = load_df(f"""
    SELECT
        category,
        COALESCE(SUM(total_revenue), 0) AS revenue,
        COALESCE(SUM(total_units_sold), 0) AS units_sold,
        COALESCE(SUM(total_transactions), 0) AS orders
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY category
    ORDER BY revenue DESC
    """)
c1, c2 = st.columns(2)
with c1:
    st.markdown(
        '<div class="chart-title">Top products by revenue</div>',
        unsafe_allow_html=True,
    )
    if product.empty:
        st.info("No product data is available.")
    else:
        fig = px.bar(
            product.sort_values("revenue"),
            x="revenue",
            y="product_name",
            orientation="h",
            custom_data=[
                "category",
                "units_sold",
                "orders",
            ],
        )
        fig.update_traces(
            marker={"color": "#4f46e5"},
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>Category: %{customdata[0]}"
                "<br>Revenue: <b>$%{x:,.2f}</b>"
                "<br>Units sold: %{customdata[1]:,.0f}"
                "<br>Orders: %{customdata[2]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=450)
        fig = style_currency_axis(fig, axis="x")
        fig.update_layout(
            xaxis_title="Revenue",
            yaxis_title=None,
            bargap=0.25,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )
with c2:
    st.markdown(
        '<div class="chart-title">Revenue by category</div>',
        unsafe_allow_html=True,
    )
    if category.empty:
        st.info("No category data is available.")
    else:
        fig = px.bar(
            category,
            x="category",
            y="revenue",
            custom_data=[
                "units_sold",
                "orders",
            ],
        )
        fig.update_traces(
            marker={"color": "#8b5cf6"},
            hovertemplate=(
                "<b>%{x}</b>"
                "<br>Revenue: <b>$%{y:,.2f}</b>"
                "<br>Units sold: %{customdata[0]:,.0f}"
                "<br>Orders: %{customdata[1]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=450)
        fig = style_currency_axis(fig, axis="y")
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Revenue",
            bargap=0.30,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )

# ============================================================
# STORE / PAYMENT PERFORMANCE

# ============================================================
section_header(
    "◫",
    "Where are sales coming from?",
    "Review store contribution and payment-method mix.",
)
store = load_df(f"""
    SELECT
        store_name,
        city AS store_city,
        country AS store_country,
        COALESCE(SUM(total_revenue), 0) AS revenue,
        COALESCE(SUM(total_transactions), 0) AS orders,
        COALESCE(SUM(total_units_sold), 0) AS units_sold
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY
        store_name,
        city,
        country
    ORDER BY revenue DESC
    """)
payment = load_df(f"""
    SELECT
        payment_method,
        COALESCE(SUM(total_transactions), 0) AS orders,
        COALESCE(SUM(total_revenue), 0) AS revenue
    FROM {SALES_AGG_TABLE}
    WHERE {sales_filter}
    GROUP BY payment_method
    ORDER BY revenue DESC
    """)
c1, c2 = st.columns(2)
with c1:
    st.markdown(
        '<div class="chart-title">Top stores by revenue</div>',
        unsafe_allow_html=True,
    )
    if store.empty:
        st.info("No store data is available.")
    else:
        fig = px.bar(
            store.head(10).sort_values("revenue"),
            x="revenue",
            y="store_name",
            orientation="h",
            custom_data=[
                "store_city",
                "store_country",
                "orders",
                "units_sold",
            ],
        )
        fig.update_traces(
            marker={"color": "#0ea5e9"},
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>City: %{customdata[0]}"
                "<br>Country: %{customdata[1]}"
                "<br>Revenue: <b>$%{x:,.2f}</b>"
                "<br>Orders: %{customdata[2]:,.0f}"
                "<br>Units sold: %{customdata[3]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=450)
        fig = style_currency_axis(fig, axis="x")
        fig.update_layout(
            xaxis_title="Revenue",
            yaxis_title=None,
            bargap=0.25,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )
with c2:
    st.markdown(
        '<div class="chart-title">Revenue by payment method</div>',
        unsafe_allow_html=True,
    )
    if payment.empty:
        st.info("No payment data is available.")
    else:
        fig = px.pie(
            payment,
            names="payment_method",
            values="revenue",
            hole=0.60,
            custom_data=["orders"],
        )
        fig.update_traces(
            textposition="outside",
            texttemplate="%{percent:.1%}",
            textfont={"size": 11},
            marker={
                "line": {
                    "color": "#ffffff",
                    "width": 2,
                }
            },
            hovertemplate=(
                "<b>%{label}</b>"
                "<br>Revenue: <b>$%{value:,.2f}</b>"
                "<br>Share: %{percent:.1%}"
                "<br>Orders: %{customdata[0]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(
            fig,
            height=450,
            show_legend=True,
        )
        fig.update_layout(
            margin={
                "l": 40,
                "r": 40,
                "t": 25,
                "b": 75,
            },
            legend={
                "orientation": "h",
                "x": 0.5,
                "xanchor": "center",
                "y": -0.08,
                "yanchor": "top",
                "title": {"text": ""},
                "font": {"size": 11},
            },
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )

# ============================================================
# CUSTOMER VALUE / SCD HISTORY

# ============================================================
section_header(
    "◉",
    "Customer value and location history",
    "Explore customer lifetime value while preserving historical customer locations.",
)
customer_history = load_df(f"""
    SELECT
        customer_id,
        customer_name,
        city,
        country,
        segment,
        effective_start,
        effective_end,
        total_orders,
        lifetime_value,
        average_order_value,
        total_items_purchased,
        first_purchase_date,
        last_purchase_date
    FROM {CUSTOMER_LTV_TABLE}
    ORDER BY lifetime_value DESC
    """)
if customer_history.empty:
    st.info("No customer lifetime data is available.")
else:
    city_summary = (
        customer_history.groupby(
            "city",
            dropna=False,
            as_index=False,
        )
        .agg(
            customers=("customer_id", "nunique"),
            revenue=("lifetime_value", "sum"),
            orders=("total_orders", "sum"),
        )
        .sort_values(
            "revenue",
            ascending=False,
        )
        .head(15)
    )
    segment_summary = (
        customer_history.groupby(
            "segment",
            dropna=False,
            as_index=False,
        )
        .agg(
            customers=("customer_id", "nunique"),
            revenue=("lifetime_value", "sum"),
        )
        .sort_values(
            "revenue",
            ascending=False,
        )
    )
    c1, c2 = st.columns(2)
    with c1:
        st.markdown(
            '<div class="chart-title">Customer value by historical city</div>',
            unsafe_allow_html=True,
        )
        fig = px.bar(
            city_summary.sort_values("revenue"),
            x="revenue",
            y="city",
            orientation="h",
            custom_data=[
                "customers",
                "orders",
            ],
        )
        fig.update_traces(
            marker={"color": "#14b8a6"},
            hovertemplate=(
                "<b>%{y}</b>"
                "<br>Lifetime value: <b>$%{x:,.2f}</b>"
                "<br>Customers: %{customdata[0]:,.0f}"
                "<br>Orders: %{customdata[1]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=450)
        fig = style_currency_axis(fig, axis="x")
        fig.update_layout(
            xaxis_title="Lifetime value",
            yaxis_title=None,
            bargap=0.25,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )
    with c2:
        st.markdown(
            '<div class="chart-title">Customer value by segment</div>',
            unsafe_allow_html=True,
        )
        fig = px.bar(
            segment_summary,
            x="segment",
            y="revenue",
            custom_data=["customers"],
        )
        fig.update_traces(
            marker={"color": "#f59e0b"},
            hovertemplate=(
                "<b>%{x}</b>"
                "<br>Lifetime value: <b>$%{y:,.2f}</b>"
                "<br>Customers: %{customdata[0]:,.0f}"
                "<extra></extra>"
            ),
        )
        fig = style_chart(fig, height=450)
        fig = style_currency_axis(fig, axis="y")
        fig.update_layout(
            xaxis_title=None,
            yaxis_title="Lifetime value",
            bargap=0.30,
        )
        st.plotly_chart(
            fig,
            use_container_width=True,
            config={
                "displaylogo": False,
                "scrollZoom": False,
                "modeBarButtonsToRemove": [
                    "lasso2d",
                    "select2d",
                ],
            },
        )
    st.markdown(
        '<div class="chart-title">Customer lifetime detail</div>',
        unsafe_allow_html=True,
    )
    page_size = st.selectbox(
        "Rows per page",
        options=[10, 25, 50, 100],
        index=1,
        key="customer_lifetime_page_size",
    )
    total_rows = len(customer_history)
    total_pages = max(
        1,
        (total_rows + page_size - 1) // page_size,
    )
    if "customer_lifetime_page" not in st.session_state:
        st.session_state.customer_lifetime_page = 1
    st.session_state.customer_lifetime_page = min(st.session_state.customer_lifetime_page, total_pages)
    st.session_state.customer_lifetime_page = max(st.session_state.customer_lifetime_page, 1)
    nav_left, nav_mid, nav_right = st.columns([1, 2, 1])
    with nav_left:
        if st.button(
            "Previous",
            use_container_width=True,
            disabled=(st.session_state.customer_lifetime_page <= 1),
            key="customer_lifetime_prev",
        ):
            st.session_state.customer_lifetime_page -= 1
            st.rerun()
    with nav_mid:
        st.markdown(
            (
                '<div class="customer-table-meta" '
                'style="text-align:center;">'
                f"Page <strong>"
                f"{st.session_state.customer_lifetime_page}"
                f"</strong> of <strong>{total_pages}</strong> · "
                f"<strong>{total_rows:,}</strong> records"
                "</div>"
            ),
            unsafe_allow_html=True,
        )
    with nav_right:
        if st.button(
            "Next",
            use_container_width=True,
            disabled=(st.session_state.customer_lifetime_page >= total_pages),
            key="customer_lifetime_next",
        ):
            st.session_state.customer_lifetime_page += 1
            st.rerun()
    start_row = (st.session_state.customer_lifetime_page - 1) * page_size
    end_row = min(
        start_row + page_size,
        total_rows,
    )
    customer_page = customer_history.iloc[start_row:end_row].copy()
    display_customer_page = customer_page.rename(
        columns={
            "customer_id": "Customer ID",
            "customer_name": "Customer Name",
            "city": "City",
            "country": "Country",
            "segment": "Segment",
            "effective_start": "Effective Start",
            "effective_end": "Effective End",
            "total_orders": "Total Orders",
            "lifetime_value": "Lifetime Value",
            "average_order_value": "Avg. Order Value",
            "total_items_purchased": "Items Purchased",
            "first_purchase_date": "First Purchase",
            "last_purchase_date": "Last Purchase",
        }
    )
    st.dataframe(
        display_customer_page,
        use_container_width=True,
        hide_index=True,
        height=min(
            620,
            42 + (len(display_customer_page) * 35),
        ),
        column_config={
            "Customer ID": st.column_config.NumberColumn(
                "Customer ID",
                format="%d",
            ),
            "Total Orders": st.column_config.NumberColumn(
                "Total Orders",
                format="%d",
            ),
            "Lifetime Value": st.column_config.NumberColumn(
                "Lifetime Value",
                format="$%.2f",
            ),
            "Avg. Order Value": st.column_config.NumberColumn(
                "Avg. Order Value",
                format="$%.2f",
            ),
            "Items Purchased": st.column_config.NumberColumn(
                "Items Purchased",
                format="%d",
            ),
            "Effective Start": st.column_config.DateColumn(
                "Effective Start",
                format="DD MMM YYYY",
            ),
            "Effective End": st.column_config.DateColumn(
                "Effective End",
                format="DD MMM YYYY",
            ),
            "First Purchase": st.column_config.DateColumn(
                "First Purchase",
                format="DD MMM YYYY",
            ),
            "Last Purchase": st.column_config.DateColumn(
                "Last Purchase",
                format="DD MMM YYYY",
            ),
        },
    )
