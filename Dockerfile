# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Build lakeFS
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM golang:1.19.2-alpine3.16 AS build

ARG VERSION=dev
ARG DUCKDB_RELEASE_TAG=v0.7.1

WORKDIR /build

# Packages required to build
RUN apk add --no-cache build-base

# Copy project deps first since they don't change often
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg go mod download

# Copy project
COPY . ./

# Build a binaries
ARG TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakefs ./cmd/lakefs
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakectl ./cmd/lakectl


# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Build delta diff binary
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM rust:1.68-alpine3.16 AS build-delta-diff-plugin
RUN apk update && apk add build-base pkgconfig openssl-dev alpine-sdk
RUN cargo new --bin delta-diff
WORKDIR /delta-diff

# 2. Copy our manifests
COPY ./pkg/plugins/diff/delta_diff_server/Cargo.lock ./Cargo.lock
COPY ./pkg/plugins/diff/delta_diff_server/Cargo.toml ./Cargo.toml

# 3. Build only the dependencies to cache them in this layer

# Rust default behavior is to build a static binary (default target is <arch>-unknown-linux-musl on Alpine, and musl
# is assumed to be static). It links to openssl statically, but these are dynamic libraries. Setting RUSTFLAGS=-Ctarget-feature=-crt-static
# forces Rust to create a dynamic binary, despite asking for musl.
RUN RUSTFLAGS=-Ctarget-feature=-crt-static cargo build --release
RUN rm src/*.rs

# 4. Now that the dependency is built, copy your source code
COPY ./pkg/plugins/diff/delta_diff_server/src ./src

# 5. Build for release.
RUN rm ./target/release/deps/delta_diff*
RUN RUSTFLAGS=-Ctarget-feature=-crt-static cargo build --release

# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Build DuckDB
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS build-duckdb

RUN apk add --no-cache git
WORKDIR /
RUN git clone https://github.com/duckdb/duckdb.git

WORKDIR /duckdb
RUN git checkout ${DUCKDB_RELEASE_TAG}

RUN apk add --no-cache build-base cmake openssl-dev ninja
RUN GEN=ninja BUILD_HTTPFS=1 make 

# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Just lakectl
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS lakectl
RUN apk add -U --no-cache ca-certificates
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build /build/lakectl ./
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Add lakefs
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS lakefs-lakectl

RUN apk add -U --no-cache ca-certificates
# Be Docker compose friendly (i.e. support wait-for)
RUN apk add netcat-openbsd

WORKDIR /app
COPY ./scripts/wait-for ./
ENV PATH /app:$PATH
COPY --from=build /build/lakefs /build/lakectl ./

EXPOSE 8000/tcp

# Setup user
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs

ENTRYPOINT ["/app/lakefs"]
CMD ["run"]

# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
# Include lakefs-plugins
# \/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/\/
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS lakefs-plugins

RUN apk add -U --no-cache ca-certificates
RUN apk add openssl-dev libc6-compat alpine-sdk
# Be Docker compose friendly (i.e. support wait-for)
RUN apk add netcat-openbsd

WORKDIR /app
COPY ./scripts/wait-for ./
ENV PATH /app:$PATH
COPY --from=build /build/lakefs /build/lakectl ./
COPY --from=build-delta-diff-plugin /delta-diff/target/release/delta_diff ./

EXPOSE 8000/tcp

# Setup user
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs

RUN mkdir -p /home/lakefs/.lakefs/plugins/diff && ln -s /app/delta_diff /home/lakefs/.lakefs/plugins/diff/delta

# Add DuckDB
USER root
WORKDIR /app
COPY --from=build-duckdb /duckdb/build/release/duckdb  ./

USER lakefs
# Create ~/.duckdbrc file to customise the prompt 🦆
RUN echo ".prompt '⚫◗ '" > $HOME/.duckdbrc

ENTRYPOINT ["/app/lakefs"]
CMD ["run"]
