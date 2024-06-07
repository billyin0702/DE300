# Use the official Airflow image as a parent image with Python 3.9
FROM apache/airflow:2.8.1-python3.9

# Switch to root to install system packages
USER root

# Install system dependencies, including gcc and required header files
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
       gcc \
       libc-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Switch back to the airflow user
USER airflow

# Install additional Python packages
COPY requirements.txt .
RUN python -m pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt