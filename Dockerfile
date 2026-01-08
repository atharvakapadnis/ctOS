# Docker file for ctOS
FROM python:3.11-slim

# Set working directory inside container
WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    sqlite3 \
    curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requiremets first for laayer caching optimizations
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY src/ ./src/
COPY streamlit_app.py .

# Copy data directory structure (empty directory)
RUN mkditr -p /app/data/input /app/data/logs /app/data/debug /app/data/rules

# Copy startup script
COPY scripts/docker-entrypoint.sh /app/docker-entrypoint.sh
RUN chmod +x /app/docker-entrypoint.sh

# Expose Streamlit default port
EXPOSE 8501

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=40s --retries=3 \
    CMD curl --fail https://localhost:8501/_stcore/health || exit 1

# Set entrypoint
ENTRYPOINT ["/app/docker-entrypoint.sh"]