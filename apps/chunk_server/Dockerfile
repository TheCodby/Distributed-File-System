# First stage: Builder
FROM rust:latest as builder
WORKDIR /usr/src

# Install protobuf-compiler in the build stage
RUN apt-get update && apt-get install -y protobuf-compiler && rm -rf /var/lib/apt/lists/*
# Copy the entire project source into the container
COPY ../../apps/chunk_server ./apps/chunk_server

# Build the project
COPY ../../libs ./libs
COPY ../../proto ./proto
WORKDIR /usr/src/apps/chunk_server
RUN cargo build --release

# Second stage: Slim image for runtime
FROM debian:bookworm

# Copy the built binary from the builder stage
COPY --from=builder /usr/src/apps/chunk_server/target/release /usr/local/bin/app
# CMD to run the binary
CMD ["/usr/local/bin/app/chunk_server"]