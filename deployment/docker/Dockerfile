# Use python-slim as base
FROM python:3.11-slim

# Install Java (required for PySpark) and curl/git
RUN mkdir -p /usr/share/man/man1 && \
    apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    curl \
    git \
    build-essential \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Set Java Home
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$PATH:$JAVA_HOME/bin

# Set working directory
WORKDIR /app

# Copy requirements and install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy package source code and install in editable mode
COPY . .
RUN pip install -e .

# Set PYTHONPATH
ENV PYTHONPATH=/app

# Default command: run pytest unit tests
CMD ["pytest", "tests/unit_tests/"]
