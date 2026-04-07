# ── Stage 1: Build frontend ──
FROM node:20-slim AS frontend-build
WORKDIR /app/frontend
COPY frontend/package.json frontend/package-lock.json* ./
RUN npm ci --no-audit --no-fund
COPY frontend/ ./
RUN npm run build

# ── Stage 2: Python backend ──
FROM python:3.12-slim AS production
WORKDIR /app

# System deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Python deps
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# App code
COPY app/ app/
COPY data/schema/ data/schema/
COPY data/parquet/ data/parquet/
COPY scripts/ scripts/

# Built frontend from stage 1
COPY --from=frontend-build /app/frontend/dist frontend/dist

# Runtime directories
RUN mkdir -p data/runtime

# Non-root user
RUN groupadd -r mop && useradd -r -g mop -s /bin/false mop
RUN chown -R mop:mop /app/data
USER mop

# Health check
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:8000/health || exit 1

EXPOSE 8000

# Gunicorn with uvicorn workers for production
CMD ["python", "-m", "uvicorn", "app.main:app", \
     "--host", "0.0.0.0", \
     "--port", "8000", \
     "--workers", "1", \
     "--log-level", "info", \
     "--access-log", \
     "--proxy-headers", \
     "--forwarded-allow-ips", "*"]
