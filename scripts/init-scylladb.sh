#!/bin/bash
set -e

echo "Waiting for ScyllaDB to become available on scylladb:9042..."
until cqlsh scylladb 9042 -e "DESCRIBE KEYSPACES" >/dev/null 2>&1; do
    echo "ScyllaDB is not available yet. Sleeping for 5 seconds..."
    sleep 5
done

echo "ScyllaDB is available. Proceeding with initialization..."

# Create keyspace and table if they do not exist.
cqlsh scylladb 9042 <<EOF
CREATE KEYSPACE IF NOT EXISTS my_keyspace WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
USE my_keyspace;
CREATE TABLE IF NOT EXISTS my_table (
    id uuid PRIMARY KEY,
    data text
);
EOF

echo "Initialization complete."
