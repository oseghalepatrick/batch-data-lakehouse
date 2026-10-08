FROM python:3.12-slim

# Copy uv directly from the official Astral image
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /bin/

WORKDIR /app

# Copy dependency file first to maximise Docker layer caching
COPY requirements.txt /app/requirements.txt

# Install dependencies into the image's Python 3.12 environment
RUN uv pip install \
    --system \
    --no-cache \
    -r /app/requirements.txt

# Copy Streamlit application
COPY app.py /app/app.py
COPY pages /app/pages
# Copy Streamlit configuration
COPY .streamlit /app/.streamlit

EXPOSE 8501

CMD ["streamlit", "run", "/app/app.py", "--server.address=0.0.0.0", "--server.port=8501"]