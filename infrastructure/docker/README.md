# Local Docker Testing Environment

This directory contains configurations to spin up a local containerized PySpark and Delta Lake environment for running unit tests.

## Prerequisites
- Docker installed
- Docker Compose installed

## Quick Start

### 1. Build and Run Tests
To build the container and execute the Pytest unit test suite locally:
```bash
docker-compose up --build
```

### 2. Interactive Shell
To launch a bash terminal inside the container to debug or run custom scripts:
```bash
docker run -it --entrypoint /bin/bash -v $(pwd)/../../:/app lakeforge-tests
```
