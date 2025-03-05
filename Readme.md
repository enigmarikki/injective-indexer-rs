# Injective Exchange Data Pipeline

## Overview
This project implements a high-performance data pipeline for processing Injective exchange data. It consists of two main components: a gRPC service for collecting data from Injective's API and a consumer service for processing and storing this data.

## Architecture

```mermaid
graph TB
    subgraph "Data Sources"
        inj[Injective Chain] --> |gRPC Stream| grpc
        inj --> |gRPC Query| heart
    end
    
    subgraph "Data Collection"
        grpc[gRPC Streaming Service]
        heart[Heartbeat Service]
    end
    
    subgraph "Message Broker"
        kafka[Kafka]
    end
    
    grpc --> |Publish Events| kafka
    heart --> |Publish Snapshots| kafka
    
    subgraph "Consumer Services"
        mkt[Market Preloader]
        dragonfly[dragonfly Consumer]
        scylla[ScyllaDB Consumer]
    end
    
    kafka --> |Market Data| mkt
    kafka --> |Positions, Trades, Orderbooks| dragonfly
    kafka --> |Market & Position Data| scylla
    
    mkt --> |Signal Markets Ready| dragonfly
    mkt --> |Signal Markets Ready| scylla
    
    subgraph "Storage"
        dragonflydb[(dragonfly)]
        scylladb[(ScyllaDB)]
    end
    
    dragonfly --> |Real-time State| dragonflydb
    scylla --> |Historical Data| scylladb
    
    subgraph "Event Distribution"
        pubsub[dragonfly PubSub]
    end
    
    dragonfly --> |Publish Updates| pubsub
    
    subgraph "Consumers"
        client1[Trading UI]
        client2[Risk Engine]
        client3[Analytics]
    end
    
    pubsub --> |Market Updates| client1
    pubsub --> |Liquidation Alerts| client2
    scylladb --> |Historical Analysis| client3
    
    classDef primary fill:#6495ED,stroke:#333,stroke-width:1px
    classDef secondary fill:#98FB98,stroke:#333,stroke-width:1px
    classDef storage fill:#FFD700,stroke:#333,stroke-width:1px
    classDef broker fill:#FF6347,stroke:#333,stroke-width:1px
    classDef client fill:#DDA0DD,stroke:#333,stroke-width:1px
    
    class inj,grpc,heart primary
    class mkt,dragonfly,scylla secondary
    class dragonflydb,scylladb,pubsub storage
    class kafka broker
    class client1,client2,client3 client
```

### Components

#### gRPC Service
- Connects to Injective's streaming and query endpoints
- Collects real-time market data (trades, orderbooks, positions)
- Periodically fetches market snapshots with heartbeat service
- Publishes all data to Kafka

#### Consumer Service
1. **Market Preloader**: 
   - Prioritizes processing market data first
   - Establishes baseline reference data before processing positions

2. **dragonfly Consumer**:
   - Maintains real-time state of markets and positions
   - Calculates liquidation prices
   - Identifies liquidatable positions
   - Publishes updates via dragonfly PubSub

3. **ScyllaDB Consumer**:
   - Stores historical market and position data
   - Maintains time-series of liquidation prices
   - Provides queryable database of liquidatable positions

### Event-Driven Architecture
The system is built on a fully event-driven architecture:

1. **Event Production**:
   - gRPC service captures exchange events (trades, orderbook updates, position changes)
   - Each event is serialized and published to Kafka topics

2. **Event Processing**:
   - Consumer services process events asynchronously
   - Processing occurs in phases to maintain data consistency
   - Each consumer handles specific event types independently
   - Events trigger calculations like liquidation prices

3. **Event Distribution**:
   - Processed events are published to dragonfly PubSub
   - Consumers can subscribe to specific event types
   - Low-latency notification for critical events like liquidations

4. **Data Flow**:
   - gRPC service collects data from Injective endpoints
   - Data is serialized and published to Kafka
   - Consumer components process data in phases:
     - Markets are processed first
     - Positions and other data follow
   - Real-time updates are published via dragonfly PubSub
   - Historical data is stored in ScyllaDB

## Features
- Fully event-driven architecture
- Loosely coupled components for scalability
- Phase-based processing to ensure data consistency
- Optimized Kafka batching for high throughput
- High-performance dragonfly PubSub for real-time updates
- Accurate liquidation price calculation
- Liquidation alerting system
- Distributed processing with specialized consumers

## Configuration
The system is configurable through environment variables or config files:

```
GRPC_STREAM_ENDPOINT=http://injective-stream:1999
GRPC_QUERY_ENDPOINT=http://injective-query:9900
KAFKA_BROKERS=kafka1:9092,kafka2:9092
KAFKA_TOPIC=injective-data
DRAGONFLY_URL=dragonfly://dragonfly:6379
SCYLLADB_NODES=scylla1:9042,scylla2:9042
```

## Deployment
The system can be deployed using Docker Compose:

```bash
# Build and start all services with a single command
docker-compose up -d
```

This will start the entire stack including:
- gRPC service
- Consumer service
- Kafka
- Dragonfly
- ScyllaDB

Everything is configured to work together out of the box.

## Requirements
- Rust 1.73+
- Kafka
- dragonfly
- ScyllaDB
- Injective API access