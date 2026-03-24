#!/bin/bash

cd "$(dirname "$0")"

# Build and run tests using the main Streaming docker-compose
# This will use the Kafka instance from the main docker-compose

cd ..

# First ensure the main services are running
docker compose ps

# Run tests against the main Kafka instance
docker run --rm \
  --network streaming-network \
  -v "$(pwd)/bytewax-consumer/tests:/app/tests" \
  -e KAFKA_BOOTSTRAP_SERVERS="kafka:9092" \
  python:3.10 bash -c "
    pip install --no-cache-dir pytest pillow kafka-python &&
    cd /app &&
    pytest tests/ -v --tb=short
  "
