#!/bin/bash

echo "Setting up directories and fixing permissions"

# Create directories if they don't exist
mkdir -p logs/{grpc-client,zookeeper,kafka,kafka-ui,dragonflydb,scylladb,scylla-init,injective-consumer}
mkdir -p zookeeper-data
mkdir -p kafka-data
mkdir -p dragonfly-data
mkdir -p scylla-data
mkdir -p injective-logs

# Fix permissions - make them accessible to all container users
echo "Setting directory permissions..."
sudo chmod -R 777 logs
sudo chmod -R 777 zookeeper-data
sudo chmod -R 777 kafka-data
sudo chmod -R 777 dragonfly-data
sudo chmod -R 777 scylla-data
sudo chmod -R 777 injective-logs

echo "Creating empty files to ensure write permissions..."
touch zookeeper-data/test.txt
touch logs/zookeeper/test.log
touch logs/kafka/test.log

echo "Setup complete! You can now run: docker-compose up -d"