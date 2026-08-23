# syntax=docker/dockerfile:1
#
# Dockerfile for the bronze -> silver -> gold pipeline
# (bronze_pipeline.py, run_dbt.py / dbt, databricks_test.py).
#
# Build context is the PROJECT ROOT, not this docker/ folder, because the
# image needs scripts/, utils/, tests/, jars/, and the dbt project. Build it
# from the project root:
#
#     docker build -f docker/Dockerfile -t data-pipeline:latest .
#
# ...or just use `docker compose build` from docker/ - compose.yml already
# points its build context at `..` for you.
#
# ASSUMPTIONS (adjust if these don't match your repo):
#   1. A pyproject.toml + uv.lock already exist at the project root (all
#      three scripts are documented as `uv run script.py`, which implies a
#      uv-managed project). If you don't have a uv.lock yet, run `uv lock`
#      at the project root once before building.
#   2. dbt-core and your Databricks adapter (dbt-databricks) are declared as
#      dependencies in that pyproject.toml. If they're not, uncomment the
#      `uv pip install dbt-core dbt-databricks` line below, or better, add
#      them properly with `uv add dbt-core dbt-databricks` and rebuild.
#   3. Your dbt profiles.yml / Databricks credentials are supplied at
#      runtime via env vars or a mounted volume (see compose.yml) - nothing
#      credential-shaped is baked into this image.

##############################################################################
# Base: Python + the JRE PySpark needs
##############################################################################
FROM python:3.11-slim AS base

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        procps \
    && rm -rf /var/lib/apt/lists/* \
    # Resolve JAVA_HOME dynamically (works on amd64 and arm64 alike, instead
    # of hardcoding an arch-specific path under /usr/lib/jvm).
    && ln -sfn "$(dirname "$(dirname "$(readlink -f "$(command -v java)")")")" /opt/java

ENV JAVA_HOME=/opt/java \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/app/.venv \
    PATH="/opt/java/bin:/app/.venv/bin:${PATH}"

# uv, straight from its official distroless image - no curl|sh install step.
COPY --from=ghcr.io/astral-sh/uv:latest /uv /uvx /usr/local/bin/

WORKDIR /app

##############################################################################
# Dependencies layer - cached separately so editing scripts/ doesn't force
# a full dependency reinstall on every build.
##############################################################################
FROM base AS deps

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev
# See ASSUMPTION 2 above - uncomment if dbt isn't already a project dependency:
# RUN uv pip install dbt-core dbt-databricks

##############################################################################
# Final image
##############################################################################
FROM deps AS final

# Whole project (scripts/, utils/, tests/, jars/, the dbt project folder,
# pipeline/, etc.) - see .dockerignore for what's excluded.
COPY . .

COPY docker/Entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

# Run as non-root.
RUN useradd --create-home --uid 1000 pipeline \
    && chown -R pipeline:pipeline /app
USER pipeline

ENTRYPOINT ["/entrypoint.sh"]
CMD ["full"]