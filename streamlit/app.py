import streamlit as st

st.set_page_config(
    page_title="Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded",
)


sales_page = st.Page(
    "pages/sales_analytics.py",
    title="Sales Analytics",
    icon="📈",
    default=True,
)

data_quality_page = st.Page(
    "pages/data_quality.py",
    title="Data Quality",
    icon="✅",
)


navigation = st.navigation(
    {
        "Analytics": [
            sales_page,
            data_quality_page,
        ]
    }
)

navigation.run()
