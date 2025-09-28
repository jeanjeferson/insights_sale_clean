# Dockerfile otimizado para Forecasting Pipeline com Darts
FROM python:3.12-slim

WORKDIR /app

# Set environment variables for Docker optimization
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    CONTAINER=true \
    PYTHONPATH=/app

# Install system dependencies including ODBC drivers and network tools
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    git \
    netcat-openbsd \
    net-tools \
    unixodbc \
    unixodbc-dev \
    odbcinst \
    libodbcinst2 \
    libodbc2 \
    unixodbc-common \
    gnupg2 \
    ca-certificates \
    pkg-config \
    libgomp1 \
    liblapack-dev \
    libopenblas-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install Microsoft ODBC Driver 18 for SQL Server
RUN curl -fsSL https://packages.microsoft.com/keys/microsoft.asc | gpg --dearmor -o /usr/share/keyrings/microsoft-prod.gpg && \
    echo "deb [arch=amd64,armhf,arm64 signed-by=/usr/share/keyrings/microsoft-prod.gpg] https://packages.microsoft.com/debian/12/prod bookworm main" > /etc/apt/sources.list.d/mssql-release.list && \
    apt-get update && \
    ACCEPT_EULA=Y apt-get install -y msodbcsql18 && \
    rm -rf /var/lib/apt/lists/*

# Verify ODBC Driver 18 installation
RUN echo "[ODBC Driver 18 for SQL Server]" >> /etc/odbcinst.ini && \
    echo "Description=Microsoft ODBC Driver 18 for SQL Server" >> /etc/odbcinst.ini && \
    echo "Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.4.so.1.1" >> /etc/odbcinst.ini && \
    echo "Threading=1" >> /etc/odbcinst.ini && \
    echo "FileUsage=1" >> /etc/odbcinst.ini && \
    echo "" >> /etc/odbcinst.ini

# List available ODBC drivers for verification
RUN odbcinst -q -d

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
