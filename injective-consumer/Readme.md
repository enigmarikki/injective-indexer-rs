# Injective Consumer Base

This is a base library for building Kafka consumers for Injective Protocol data. It provides:

- Core Kafka consumer implementation
- Data models for Injective Protocol data
- Configuration handling
- Base types and traits

## Features

- **MessageProcessor Trait**: Define your own message processors that implement this trait to handle Kafka messages
- **KafkaConsumer**: A reusable Kafka consumer that can be configured with different message processors
- **Data Models**: Complete set of data models for Injective Protocol data

## Usage

Add this library to your Cargo.toml:

```toml
[dependencies]
injective-consumer-base = "0.1.0"
```

Then implement the `MessageProcessor` trait for your own processor:

```rust
use injective_consumer_base::{KafkaMessage, MessageProcessor};
use std::error::Error;

struct MyProcessor;

impl MessageProcessor for MyProcessor {
    fn process_message(&self, message: KafkaMessage) -> Result<(), Box<dyn Error + Send + Sync>> {
        // Process the message...
        Ok(())
    }
}
```

Create a Kafka consumer with your processor:

```rust
use injective_consumer_base::{Config, KafkaConsumer};
use std::sync::Arc;

async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    // Load configuration
    let config = Config::from_env()?;
    
    // Create processor
    let processor = Arc::new(MyProcessor);
    
    // Create consumer
    let consumer = KafkaConsumer::new(&config.kafka, "my-group-id", processor)?;
    
    // Start consuming messages
    consumer.start().await?;
    
    Ok(())
}
```

## License

This project is licensed under the MIT License - see the LICENSE file for details.