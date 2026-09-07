# Gate — the egress rate limiter, as one artefact.
#
# The console is compiled INTO the binary (rust-embed over ui/dist), so the
# frontend stage is a build dependency and not packaging: without it the Rust
# compile hard-errors on a missing folder. That is deliberate — a deployment is
# one image, and there is no static bundle to serve alongside it.
#
# Build: DOCKER_BUILDKIT=1 docker build -t gate .
# Run:   docker run -p 8788:8788 -e QUEEN_URL=http://queen:6632 gate

# ---------------------------------------------------------------- the console
FROM node:24-alpine AS ui-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# ------------------------------------------------------------------ the gate
FROM rust:1-bookworm AS server-builder

WORKDIR /usr/build

# Manifests first, then `cargo fetch`, so a source-only change re-uses the
# downloaded registry instead of resolving the graph again.
#
# The pre-copy used to sit here with no fetch between it and `COPY crates`,
# which bought nothing at all — the next layer replaced every manifest it had
# just copied. It also missed `crates/bench`, so the copy could not have been
# complete even if it had been useful. Both fixed: every member is listed, and
# the fetch is what makes listing them worth doing.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml ./crates/core/
COPY crates/server/Cargo.toml ./crates/server/
COPY crates/e2e/Cargo.toml ./crates/e2e/
COPY crates/bench/Cargo.toml ./crates/bench/
RUN mkdir -p crates/core/src crates/server/src crates/e2e/src crates/bench/src \
    && echo "" > crates/core/src/lib.rs \
    && echo "fn main() {}" > crates/server/src/main.rs \
    && echo "fn main() {}" > crates/e2e/src/main.rs \
    && echo "fn main() {}" > crates/bench/src/main.rs
RUN --mount=type=cache,target=/usr/local/cargo/registry cargo fetch --locked

COPY crates ./crates

# webapp.rs embeds `../../ui/dist` with rust_embed, which fails the compile when
# the folder is missing. crates/server/build.rs also declares it as a build
# input, so a rebuilt console with unchanged Rust still produces a new binary
# instead of silently serving the previous one.
COPY --from=ui-builder /app/ui/dist ./ui/dist

RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/usr/build/target \
    cargo build --release --bin gate-server && cp target/release/gate-server /gate-server

# ---------------------------------------------------------------- the runtime
FROM debian:bookworm-slim

# What CI passes as --label, repeated here because the first push is by hand:
# without it the package on GHCR is an orphan — no link to the repository and
# none of its visibility — until the pipeline republishes it.
LABEL org.opencontainers.image.source="https://github.com/queen-mq/gate" \
      org.opencontainers.image.description="Gate — egress rate limiter on QueenMQ" \
      org.opencontainers.image.licenses="Apache-2.0"

RUN apt-get update \
    && apt-get install -y --no-install-recommends ca-certificates libssl3 \
    && rm -rf /var/lib/apt/lists/*

# The same uid the rest of the stack runs as, and the reason the chart can set
# readOnlyRootFilesystem: nothing is written to disk. Config lives in queen.kv,
# history in Postgres, and the console is inside the binary.
RUN groupadd -g 10001 gate && useradd -u 10001 -g 10001 -M -s /usr/sbin/nologin gate

WORKDIR /app
COPY --from=server-builder /gate-server ./bin/gate-server

USER 10001:10001

# The internal listener. The public one is opt-in via GATE_PUBLIC_BIND, because
# a control plane that binds a public port by default is a control plane that
# gets exposed by accident.
EXPOSE 8788

CMD ["./bin/gate-server"]
