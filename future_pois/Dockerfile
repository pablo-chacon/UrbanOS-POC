# Use a Python image as the base
FROM python:3.12-slim

# Set the working directory inside the container
WORKDIR /app

# Install system dependencies for PostgreSQL and Python build tools
RUN apt-get update && \
    apt-get install -y \
    libpq-dev gcc build-essential && \
    rm -rf /var/lib/apt/lists/*

# Copy the requirements.txt file and install Python dependencies
COPY requirements.txt /app/requirements.txt
# Upgrade pip before installing requirements
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code to the container
COPY . /app

# Set environment variables for consistent behavior
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH "${PYTHONPATH}:/app"

# Default command to run the main automation script
CMD ["python", "main.py"]
