# Use a Python image as the base
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

COPY requirements.txt /app/requirements.txt

# Install system dependencies and Python requirements
RUN apt-get update && apt-get install -y \
    libpq-dev gcc build-essential && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application files to the container
COPY . /app

# Ensure the /data directory is accessible
VOLUME /data

# Set environment variables for consistent behavior
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Default command to run the main script
CMD ["python", "main.py"]
