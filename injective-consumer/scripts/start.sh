#!/bin/bash
set -e

# Create necessary directories
mkdir -p config scripts

# Make sure the initialization script is executable
chmod +x scripts/init-scylladb.sh

# Build and start the services
echo "Starting the Injective processing environment..."
docker-compose up -d

# Check service status
echo "Checking service status..."
docker-compose ps

echo "Environment is starting up!"
echo "Using external Kafka at: ${KAFKA_BROKERS:-host.docker.internal:9092}"
echo "You can check logs with: docker-compose logs -f"