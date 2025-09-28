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

# Copy main application files
COPY run_all_pipelines.py ./

# Copy pipelines directory
COPY pipelines/ ./pipelines/

# Copy utils directory
COPY utils/ ./utils/

# Copy api directory
COPY api/ ./api/

# Copy configuration files
COPY config/ ./config/

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
    CMD python -c "import darts, pandas, numpy, lightgbm, yaml, fastapi, uvicorn; print('Environment OK')" || exit 1

# Expose port for API
EXPOSE 8000

# Default command - starts the FastAPI server
CMD ["python", "-m", "uvicorn", "api.main:app", "--host", "0.0.0.0", "--port", "8000"]
