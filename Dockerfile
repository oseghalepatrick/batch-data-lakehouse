FROM apache/airflow:3.3.2

ADD airflow/requirements.txt requirements.txt
ADD airflow/soda_requirements.txt soda_requirements.txt

RUN uv pip install -r requirements.txt

USER root

RUN apt-get update && \
    apt-get install -y \
        build-essential \
        gcc \
        g++ \
        libc6-dev \
        pkg-config \
        curl && \
    rm -rf /var/lib/apt/lists/*

USER airflow

ENV PATH="/home/airflow/.local/bin:/home/airflow/.cargo/bin:${PATH}"
ENV CC=/usr/bin/gcc
ENV CXX=/usr/bin/g++
ENV CARGO_TARGET_AARCH64_UNKNOWN_LINUX_GNU_LINKER=/usr/bin/gcc

# Install Python 3.11 and create isolated Soda environment
RUN uv python install 3.11.8 && \
    uv venv \
        --python 3.11.8 \
        /opt/airflow/soda_venv

# Install Rust toolchain for dask-sql build
RUN curl --proto '=https' \
        --tlsv1.2 \
        -sSf https://sh.rustup.rs | \
        sh -s -- -y

# Install all Soda/Dask dependencies specifically into the Python 3.11 venv
RUN . "$HOME/.cargo/env" && \
    uv pip install \
        --python /opt/airflow/soda_venv/bin/python \
        setuptools-rust && \
    uv pip install \
        --python /opt/airflow/soda_venv/bin/python \
        "dask[complete]==2022.11.1" && \
    uv pip install \
        --python /opt/airflow/soda_venv/bin/python \
        --no-build-isolation \
        "dask-sql==2022.12.0" && \
    uv pip install \
        --python /opt/airflow/soda_venv/bin/python \
        -r /opt/airflow/soda_requirements.txt

ENV PYTHONPATH=/opt/airflow
ENV DBT_PROFILES_DIR=/opt/airflow/dbt/profiles
ENV DBT_PROJECT_DIR=/opt/airflow/dbt/iceberg_datalake

USER airflow