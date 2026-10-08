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
    """Load dashboard filter values from fact_sales."""

    stores = load_df("""
        SELECT DISTINCT store_name
        FROM fact_sales
        WHERE store_name IS NOT NULL
        ORDER BY store_name
        """)["store_name"].tolist()

    categories = load_df("""
        SELECT DISTINCT category
        FROM fact_sales
        WHERE category IS NOT NULL
        ORDER BY category
        """)["category"].tolist()

    countries = load_df("""
        SELECT DISTINCT store_country
        FROM fact_sales
        WHERE store_country IS NOT NULL
        ORDER BY store_country
        """)["store_country"].tolist()

    return stores, categories, countries


@st.cache_data(ttl=300)
def get_max_transaction_date():
    """
    Get the latest transaction date available in fact_sales.

    This is used as the dashboard's effective current date instead
    of Python's date.today().
    """

    result = load_df("""
        SELECT MAX(transaction_date) AS max_date
        FROM fact_sales
        """)

    max_date = result.iloc[0]["max_date"]

    if pd.isna(max_date):
        raise ValueError("fact_sales contains no transaction dates.")

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


# ============================================================
# LOAD FILTER VALUES / CONNECTION CHECK
# ============================================================

try:
    stores, categories, countries = load_filter_values()
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


selected_stores = st.sidebar.multiselect(
    "Store",
    stores,
)

selected_categories = st.sidebar.multiselect(
    "Category",
    categories,
)

selected_countries = st.sidebar.multiselect(
    "Country",
    countries,
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
# CURRENT PERIOD FILTER
# ============================================================

where = [f"""
    transaction_date >= DATE '{start_date}'
    AND transaction_date <= DATE '{end_date}'
    """]

if selected_stores:
    where.append(f"store_name IN ({sql_string_list(selected_stores)})")

if selected_categories:
    where.append(f"category IN ({sql_string_list(selected_categories)})")

if selected_countries:
    where.append(f"store_country IN ({sql_string_list(selected_countries)})")

if statuses:
    where.append(f"status IN ({sql_string_list(statuses)})")

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
# PREVIOUS PERIOD FILTER
# ============================================================

previous_where = [f"""
    transaction_date >= DATE '{kpi_previous_start}'
    AND transaction_date <= DATE '{kpi_previous_end}'
    """]

if selected_stores:
    previous_where.append(f"store_name IN ({sql_string_list(selected_stores)})")

if selected_categories:
    previous_where.append(f"category IN ({sql_string_list(selected_categories)})")

if selected_countries:
    previous_where.append(f"store_country IN ({sql_string_list(selected_countries)})")

if statuses:
    previous_where.append(f"status IN ({sql_string_list(statuses)})")

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
            Trino / {TRINO_CATALOG}.{TRINO_SCHEMA}
        </strong>
    </div>

    <div class="analysis-period">
        <strong>Analysis period:</strong>
        {start_date} to {end_date}
    </div>

    <div class="kpi-period">
        <strong>KPI comparison:</strong>
        {kpi_current_start} to {kpi_current_end}
        &nbsp;vs&nbsp;
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
        transaction_date AS sales_date,
        SUM(total_amount) AS revenue,
        COUNT(DISTINCT transaction_id) AS orders,
        SUM(quantity) AS units,
        AVG(total_amount) AS average_order_value
    FROM fact_sales
    WHERE {sales_filter}
    GROUP BY transaction_date
    ORDER BY transaction_date
    """)


# ============================================================
# CURRENT KPI VALUES
# ============================================================

current_kpis = load_df(f"""
    SELECT
        COALESCE(SUM(total_amount), 0) AS total_revenue,
        COUNT(DISTINCT transaction_id) AS total_orders,
        COALESCE(SUM(quantity), 0) AS units_sold,
        COALESCE(AVG(total_amount), 0) AS avg_order_value
    FROM fact_sales
    WHERE {sales_filter}
    """).iloc[0]


# ============================================================
# PREVIOUS KPI VALUES
# ============================================================

previous_kpis = load_df(f"""
    SELECT
        COALESCE(SUM(total_amount), 0) AS total_revenue,
        COUNT(DISTINCT transaction_id) AS total_orders,
        COALESCE(SUM(quantity), 0) AS units_sold,
        COALESCE(AVG(total_amount), 0) AS avg_order_value
    FROM fact_sales
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
        )

        fig.update_traces(
            line={"width": 3},
            marker={"size": 7},
        )

        fig.update_layout(
            height=390,
            margin={
                "l": 20,
                "r": 20,
                "t": 20,
                "b": 20,
            },
            template="plotly_white",
            xaxis_title=None,
            yaxis_title="Revenue",
            hovermode="x unified",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
        )

with c2:
    st.markdown(
        '<div class="chart-title">Daily orders</div>',
        unsafe_allow_html=True,
    )

    if daily.empty:
        st.info("No order data is available for the selected filters.")
    else:
        fig = px.bar(
            daily,
            x="sales_date",
            y="orders",
        )

        fig.update_layout(
            height=390,
            margin={
                "l": 20,
                "r": 20,
                "t": 20,
                "b": 20,
            },
            template="plotly_white",
            xaxis_title=None,
            yaxis_title="Orders",
            hovermode="x unified",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
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
        product_id,
        product_name,
        category,
        SUM(quantity) AS units_sold,
        SUM(total_amount) AS revenue,
        COUNT(DISTINCT transaction_id) AS orders
    FROM fact_sales
    WHERE {sales_filter}
    GROUP BY
        product_id,
        product_name,
        category
    ORDER BY revenue DESC
    LIMIT 15
    """)

category = load_df(f"""
    SELECT
        category,
        SUM(total_amount) AS revenue,
        SUM(quantity) AS units_sold,
        COUNT(DISTINCT transaction_id) AS orders
    FROM fact_sales
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
            hover_data=[
                "category",
                "units_sold",
                "orders",
            ],
        )

        fig.update_layout(
            height=450,
            margin={
                "l": 20,
                "r": 20,
                "t": 20,
                "b": 20,
            },
            template="plotly_white",
            xaxis_title="Revenue",
            yaxis_title=None,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
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
            hover_data=[
                "units_sold",
                "orders",
            ],
        )

        fig.update_layout(
            height=450,
            margin={
                "l": 20,
                "r": 20,
                "t": 20,
                "b": 20,
            },
            template="plotly_white",
            xaxis_title=None,
            yaxis_title="Revenue",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
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
        store_city,
        store_country,
        SUM(total_amount) AS revenue,
        COUNT(DISTINCT transaction_id) AS orders,
        SUM(quantity) AS units_sold
    FROM fact_sales
    WHERE {sales_filter}
    GROUP BY
        store_name,
        store_city,
        store_country
    ORDER BY revenue DESC
    """)

payment = load_df(f"""
    SELECT
        payment_method,
        COUNT(DISTINCT transaction_id) AS orders,
        SUM(total_amount) AS revenue
    FROM fact_sales
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
            hover_data=[
                "store_city",
                "store_country",
                "orders",
                "units_sold",
            ],
        )

        fig.update_layout(
            height=450,
            margin=dict(
                l=20,
                r=20,
                t=20,
                b=20,
            ),
            template="plotly_white",
            xaxis_title="Revenue",
            yaxis_title=None,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
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
            hole=0.55,
        )

        fig.update_layout(
            height=450,
            margin=dict(
                l=20,
                r=20,
                t=20,
                b=20,
            ),
            template="plotly_white",
            legend_title=None,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
        )


# ============================================================
# CUSTOMER VALUE / SCD HISTORY
# ============================================================

section_header(
    "◉",
    "Customer value and location history",
    "Explore customer lifetime value while preserving historical customer locations.",
)

customer_history = load_df("""
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
    FROM agg_customer_lifetime_value
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
            hover_data=[
                "customers",
                "orders",
            ],
        )

        fig.update_layout(
            height=450,
            margin=dict(
                l=20,
                r=20,
                t=20,
                b=20,
            ),
            template="plotly_white",
            xaxis_title="Lifetime value",
            yaxis_title=None,
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
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
            hover_data=["customers"],
        )

        fig.update_layout(
            height=450,
            margin={
                "l": 20,
                "r": 20,
                "t": 20,
                "b": 20,
            },
            template="plotly_white",
            xaxis_title=None,
            yaxis_title="Lifetime value",
        )

        st.plotly_chart(
            fig,
            use_container_width=True,
            config={"displaylogo": False},
        )

    st.markdown(
        '<div class="chart-title">Customer lifetime detail</div>',
        unsafe_allow_html=True,
    )

    st.dataframe(
        customer_history,
        use_container_width=True,
        hide_index=True,
    )
