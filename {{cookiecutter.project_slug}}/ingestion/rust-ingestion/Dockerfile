FROM rust:bookworm as builder

WORKDIR /app
COPY . .

# Generate Cargo.lock avoid conflicts
RUN rm -f Cargo.lock && cargo generate-lockfile

# Compile binary release
RUN cargo build --release

FROM debian:bookworm-slim

# Install dependencies binary Rust
RUN apt-get update && apt-get install -y \
    ca-certificates \
    libssl3 \
    && rm -rf /var/lib/apt/lists/*

# Copy compiled binary
COPY --from=builder /app/target/release/rust-ingestion /usr/local/bin/rust-ingestion

# Set executable
RUN chmod +x /usr/local/bin/rust-ingestion

CMD ["rust-ingestion"]