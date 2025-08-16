FROM python:3.10-slim

# Set working directory
WORKDIR /app

# Pre-install system dependencies for Geo + PyGEO stack
RUN apt-get update && apt-get install -y \
    libpq-dev gcc build-essential \
    libgeos-dev libproj-dev libgdal-dev g++ curl \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (for better Docker cache)
COPY requirements.txt .

# Upgrade pip and install dependencies
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy source code after dependencies
COPY . .

# Set environment variables
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH=/app

CMD ["python", "main.py"]
