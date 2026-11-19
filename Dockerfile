# ADB server image.
#
#   docker build -t adb .                                     # AQL parser from GitHub
#   docker build -t adb --build-context aql=../AQL .          # AQL parser from a local checkout
#
# The `aql` stage below is empty unless a build context of that name replaces it.
FROM scratch AS aql

FROM rust:1-trixie AS builder
RUN apt-get update && apt-get install -y --no-install-recommends \
    cmake pkg-config libssl-dev protobuf-compiler \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=aql . /AQL
COPY . .
# A local AQL checkout, when given, patches the git dependency (as .cargo/config.toml does
# for local development).
RUN if [ -f /AQL/crates/aql-parser/Cargo.toml ]; then \
      mkdir -p .cargo && printf '[patch."https://github.com/a3t-ai/AQL.git"]\naql-parser = { path = "/AQL/crates/aql-parser" }\n' > .cargo/config.toml; \
    fi
RUN cargo build --release -p adb-cli

FROM debian:trixie-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=builder /app/target/release/adb /usr/local/bin/adb
EXPOSE 3000
CMD ["adb", "serve-http", "--port", "3000"]
