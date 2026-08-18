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
FROM node:22-alpine AS ui-builder

WORKDIR /app/ui
COPY ui/package*.json ./
RUN npm ci
COPY ui/ ./
RUN npm run build

# ------------------------------------------------------------------ the gate
FROM rust:1-bookworm AS server-builder

WORKDIR /usr/build

# Manifests first, so a source-only change does not re-resolve the graph.
COPY Cargo.toml Cargo.lock ./
COPY crates/core/Cargo.toml ./crates/core/
COPY crates/server/Cargo.toml ./crates/server/
COPY crates/e2e/Cargo.toml ./crates/e2e/

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

# Quello che la CI passa come --label. Ripetuto qui perché il primo push è a
# mano: senza, il pacchetto su GHCR resta orfano — nessun link al repository,
# nessuna eredità della sua visibilità — finché non lo ripubblica la pipeline.
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
