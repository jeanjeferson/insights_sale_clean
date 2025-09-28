# Dockerfile otimizado para Forecasting Pipeline com Darts
FROM python:3.11-slim

WORKDIR /app

# Set environment variables for Docker optimization
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONTAINER=true \
    PYTHONPATH=/app

# Install system dependencies including database drivers and ML libraries
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    unixodbc-dev \
    freetds-dev \
    freetds-bin \
    tdsodbc \
    ca-certificates \
    pkg-config \
    libgomp1 \
    liblapack-dev \
    libopenblas-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Copy dependency files first (for better Docker layer caching)
COPY requirements.txt ./

# Install Python dependencies
RUN pip3 install --upgrade pip && \
    pip3 install --upgrade --no-cache-dir -r requirements.txt

# Copy environment file (if exists)
COPY .env* ./

# Copy entrypoint script
COPY docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

# Copy main application files
COPY sql_query.py ./
COPY run_forecast_vendas.py ./
COPY run_forecast_volume.py ./
COPY ftp_uploader.py ./
COPY api_forecast.py ./
COPY api_usage.py ./

# Copy configuration files
COPY config_databases.yaml ./
COPY config_vendas.yaml ./
COPY config_volume_grupo.yaml ./

# Copy SQL directory
COPY sql ./sql/

# Create runtime directories with proper structure
RUN mkdir -p ./dataset \
             ./results

# Set proper permissions
RUN chmod -R 755 /app

# Create non-root user for security
RUN useradd --create-home --shell /bin/bash forecaster && \
    chown -R forecaster:forecaster /app
USER forecaster

# Health check to verify the environment is ready
HEALTHCHECK --interval=300s --timeout=60s --start-period=30s --retries=2 \
    CMD python -c "import darts, pandas, numpy, lightgbm; print('Environment OK')" || exit 1

# Set entrypoint
ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]

# Default command - runs the sales forecasting pipeline  
CMD ["sales"]
