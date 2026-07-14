FROM node:22-bookworm-slim AS frontend-build

WORKDIR /app/frontend
COPY frontend/package*.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build


FROM python:3.12-slim AS runtime

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1

WORKDIR /app

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --upgrade pip \
    && pip install -r requirements.txt

COPY app ./app
COPY data/schema ./data/schema
COPY data/parquet ./data/parquet
COPY data/boundaries ./data/boundaries
COPY data/uploads ./data/uploads
COPY data/verified_queries.yaml ./data/verified_queries.yaml
COPY --from=frontend-build /app/frontend/dist ./frontend/dist

RUN mkdir -p /app/data/runtime

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=30s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8000/health >/dev/null || exit 1

# WEB_CONCURRENCY: uvicorn worker processes. At 40-50 concurrent users, 4 is
# a safe default — LLM calls are I/O-bound so most time is spent waiting, and
# multi-worker prevents one long reasoning-mode query from blocking everyone.
# Tune higher if reasoning-mode adoption climbs.
ENV WEB_CONCURRENCY=4
CMD ["sh", "-c", "python -m uvicorn app.main:app --host 0.0.0.0 --port 8000 --workers ${WEB_CONCURRENCY}"]
