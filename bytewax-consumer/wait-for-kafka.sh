#!/bin/bash

# Wait until Kafka is ready
echo "Waiting for Kafka to be ready at ${KAFKA_BROKERS}..."

# Extract host and port
IFS=':' read -r HOST PORT <<< "$KAFKA_BROKERS"

# Wait for TCP connection
until nc -z "$HOST" "$PORT" 2>/dev/null; do
  echo "Kafka is unavailable - sleeping"
  sleep 1
done

echo "Kafka TCP port is open. Waiting for broker to be fully initialized..."
# Wait for broker to be fully ready (Kafka needs time to initialize metadata)
sleep 15

echo "Executing bytewax"
exec uv run python -m bytewax.run app.main "$@"



