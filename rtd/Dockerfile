# Use Python official image as a base
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies needed for psycopg and libpq
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt first to leverage Docker cache
COPY requirements.txt ./
RUN python -m pip install --upgrade pip
# Install dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the entire content of the data_loader module
COPY . /app

# Set environment variables for consistent behavior
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Specify the command to run the data loader script
ENTRYPOINT ["python", "main.py"]
