# syntax=docker/dockerfile:1.7
#
# tsdb-migrate-server: HTTP control plane for tsdb-migrate.
# Build context MUST be the repository root: the server embeds web/ via
# include_str!(concat!(env!("CARGO_MANIFEST_DIR"), "/../../web/...")).
#
#   docker build -t tsdb-migrate-server:0.1.0 .
#   docker run --rm -p 8080:8080 \
#     -e TSDB_MIGRATE_TOKEN=change-me \
#     -v tsdb-migrate-data:/data \
#     tsdb-migrate-server:0.1.0

ARG RUST_VERSION=1
FROM rust:${RUST_VERSION}-bookworm AS builder

# aws-lc-sys (pulled in by object_store/reqwest through rustls) builds C/asm
# sources and needs CMake plus a C toolchain; NASM is used on x86_64.
RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      ca-certificates \
      cmake \
      clang \
      nasm \
      pkg-config \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /src

ENV CARGO_TERM_COLOR=always \
    CARGO_INCREMENTAL=0

# Only the manifests are copied first so dependency resolution stays cached
# independently of source edits.
COPY Cargo.toml Cargo.lock ./
COPY crates ./crates
COPY web ./web

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/local/cargo/git \
    --mount=type=cache,target=/src/target \
    cargo build --locked --release -p tsdb-migrate-server \
 && install -D -m 0755 /src/target/release/tsdb-migrate-server /out/tsdb-migrate-server

FROM debian:bookworm-slim AS runtime

# curl is only for HEALTHCHECK; drop it together with the HEALTHCHECK line if
# your orchestrator probes /api/health itself.
RUN apt-get update \
 && apt-get install -y --no-install-recommends ca-certificates curl \
 && rm -rf /var/lib/apt/lists/* \
 && groupadd --system --gid 10001 migrate \
 && useradd --system --uid 10001 --gid migrate --home-dir /data --shell /usr/sbin/nologin migrate \
 && mkdir -p /data/state \
 && chown -R migrate:migrate /data

COPY --from=builder /out/tsdb-migrate-server /usr/local/bin/tsdb-migrate-server

LABEL org.opencontainers.image.title="tsdb-migrate-server" \
      org.opencontainers.image.description="HTTP control plane for tsdb-migrate" \
      org.opencontainers.image.licenses="Apache-2.0"

ENV RUST_LOG=info,tower_http=warn,axum=warn

WORKDIR /data
USER migrate
EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=3s --start-period=10s --retries=3 \
  CMD curl -fsS http://127.0.0.1:8080/api/health || exit 1

# The server only traps SIGINT (tokio::signal::ctrl_c). With the default SIGTERM
# docker would kill the process and active jobs would not be paused/persisted,
# so stop it with SIGINT. In Kubernetes set stopSignal: SIGINT as well (or add
# a SIGTERM handler in main.rs).
STOPSIGNAL SIGINT

ENTRYPOINT ["/usr/local/bin/tsdb-migrate-server"]
CMD ["--bind", "0.0.0.0:8080", "--state-dir", "/data/state", "--allow-root", "/data"]
