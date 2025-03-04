FROM rust:1.73-slim as builder

# Create a working directory
WORKDIR /app

# Install dependencies
RUN apt-get update && \
    apt-get install -y libssl-dev pkg-config && \
    rm -rf /var/lib/apt/lists/*

# Copy over your manifests
COPY Cargo.toml Cargo.lock ./

# This is a trick to cache dependencies
# Create a dummy main.rs that does nothing
RUN mkdir -p src && \
    echo "fn main() {}" > src/main.rs && \
    cargo build --release && \
    rm -f target/release/deps/injective*

# Now copy your actual source code
COPY . .

# Build for release
RUN cargo build --release

# Final lightweight image
FROM debian:bullseye-slim

# Install runtime dependencies
RUN apt-get update && \
    apt-get install -y ca-certificates libssl1.1 && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy the binary from builder
COPY --from=builder /app/target/release/injective-consumer /app/injective-consumer

# Create a directory for config
RUN mkdir -p /app/config

# Set environment variables
ENV RUST_LOG=info

# Run the binary
CMD ["/app/injective-consumer"]