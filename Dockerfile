# ---------- Base image ----------
FROM python:3.12-slim

# ---------- Set working directory ----------
WORKDIR /app

# ---------- Copy project files ----------
COPY . .

# ---------- Install system dependencies ----------
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    git \
    && rm -rf /var/lib/apt/lists/*

# ---------- Install dbt and dependencies ----------
RUN pip install --no-cache-dir --upgrade pip && \
    pip install dbt-core==1.8.3 dbt-duckdb==1.8.3

# ---------- (Optional) install dbt-utils and other packages ----------
RUN dbt deps

# ---------- Set environment variables ----------
ENV DBT_PROFILES_DIR=/app

# ---------- Default command ----------
CMD ["dbt", "run"]
