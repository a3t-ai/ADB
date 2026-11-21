# ADB server image, and the release binary for Linux.
#
#   docker build -t adb .                                       # plain: no console, no model
#   docker build -t adb --build-context aql=../AQL .            # AQL parser from a local checkout
#   docker build -t adb --build-arg ADB_FEATURES=console --build-arg ADB_MODEL=1 --build-arg ADB_PROFILE=dist .
#                                                               # the published image: console + local model
#
# The binary is built on Ubuntu 24.04, so the same file runs on any Linux with glibc
# 2.39 or newer (Ubuntu 24.04+, Debian 13+). That floor comes from ONNX Runtime's
# prebuilt library (the local embedding model), which needs glibc 2.38 and GCC 14's
# libstdc++. The release workflow copies the binary out of the builder stage. A3T's images build without the console: its ADB serves no UI.
#
# The `aql` stage below is empty unless a build context of that name replaces it.
FROM scratch AS aql

FROM ubuntu:24.04 AS builder
ARG DEBIAN_FRONTEND=noninteractive
# GCC 14 as cc/c++: ONNX Runtime's prebuilt library is built with it.
RUN apt-get update && apt-get install -y --no-install-recommends \
      ca-certificates curl build-essential gcc-14 g++-14 cmake pkg-config libssl-dev unzip \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/gcc-14 /usr/local/bin/cc && ln -sf /usr/bin/gcc-14 /usr/local/bin/gcc \
    && ln -sf /usr/bin/g++-14 /usr/local/bin/c++ && ln -sf /usr/bin/g++-14 /usr/local/bin/g++
# A current protoc release (independent of the distribution's).
ARG PROTOC_VERSION=29.3
RUN arch=$(uname -m | sed 's/aarch64/aarch_64/') \
    && curl -fsSL -o /tmp/protoc.zip "https://github.com/protocolbuffers/protobuf/releases/download/v${PROTOC_VERSION}/protoc-${PROTOC_VERSION}-linux-${arch}.zip" \
    && unzip -q /tmp/protoc.zip -d /usr/local && rm /tmp/protoc.zip
RUN curl -fsSL https://sh.rustup.rs | sh -s -- -y --profile minimal --default-toolchain stable
ENV PATH=/root/.cargo/bin:$PATH
WORKDIR /app
COPY --from=aql . /AQL
COPY . .
# A local AQL checkout, when given, patches the git dependency (as .cargo/config.toml does
# for local development).
RUN if [ -f /AQL/crates/aql-parser/Cargo.toml ]; then \
      mkdir -p .cargo && printf '[patch."https://github.com/a3t-ai/AQL.git"]\naql-parser = { path = "/AQL/crates/aql-parser" }\n' > .cargo/config.toml; \
    fi
ARG ADB_FEATURES=""
# ADB_PROFILE=dist (published builds): the perf profile, the build the published
# benchmarks ran on, without symbols; a fraction of fat LTO's build time and memory.
ARG ADB_PROFILE=release
ENV CARGO_INCREMENTAL=0
RUN cargo build --profile "$ADB_PROFILE" -p adb-cli ${ADB_FEATURES:+--features $ADB_FEATURES} \
    && cp "target/$ADB_PROFILE/adb" /adb && rm -rf target
# ADB_MODEL=1: bake the local embedding model in, so similarity search works offline.
ARG ADB_MODEL=""
RUN mkdir -p /models && if [ -n "$ADB_MODEL" ]; then bash tests/fetch-model.sh /models/bge-small-en-v1.5; fi

FROM ubuntu:24.04
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
    && rm -rf /var/lib/apt/lists/* \
    && useradd --system --uid 10001 --home-dir /data adb \
    && mkdir -p /data && chown adb /data
COPY --from=builder /adb /usr/local/bin/adb
COPY --from=builder /models /models
USER adb
VOLUME /data
EXPOSE 3000 3001
CMD ["adb", "serve-http", "--port", "3000", "--data-dir", "/data"]
