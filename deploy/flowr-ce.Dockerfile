# FlowR-CE for ADB integration tests (see docker-compose.test.yml). Built from a local
# flowr-system checkout (main context) with AQL as the `aql` build context, which
# FlowR-CE's Cargo.toml expects at ../../AQL.
FROM rust:1-trixie AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev protobuf-compiler clang git \
    && rm -rf /var/lib/apt/lists/*
COPY --from=aql . /AQL
COPY shared /build/shared
COPY proto /build/proto
COPY flowr-ce/Cargo.toml flowr-ce/Cargo.lock /build/flowr-ce/
COPY flowr-ce/crates /build/flowr-ce/crates
WORKDIR /build/flowr-ce
RUN cargo build --release -p flowr-ce

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3t64 \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /build/flowr-ce/target/release/flowr-ce /usr/local/bin/flowr-ce
ENV CLAWDFLOW_DATA_DIR=/data/flowr RUST_LOG=warn
EXPOSE 3002
CMD ["flowr-ce", "--mode", "http", "--port", "3002"]
