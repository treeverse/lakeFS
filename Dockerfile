FROM --platform=$BUILDPLATFORM golang:1.19.2-alpine AS build

ARG VERSION=dev

WORKDIR /build

# Packages required to build
RUN apk add --no-cache build-base

# Copy project deps first since they don't change often
COPY go.mod go.sum ./
RUN go mod download

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

# Build delta diff binary
FROM --platform=$BUILDPLATFORM rust:1.68-bullseye AS build-delta-diff-plugin
RUN USER=root cargo new --bin delta-diff
WORKDIR /delta-diff

# 2. Copy our manifests
COPY ./pkg/plugins/diff/delta_diff_server/Cargo.lock ./Cargo.lock
COPY ./pkg/plugins/diff/delta_diff_server/Cargo.toml ./Cargo.toml

# 3. Build only the dependencies to cache them in this layer
RUN cargo build --release
RUN rm src/*.rs

# 4. Now that the dependency is built, copy your source code
COPY ./pkg/plugins/diff/delta_diff_server/src ./src

# 5. Build for release.
RUN rm ./target/release/deps/delta_diff*
RUN cargo build --release

# lakectl image
FROM --platform=$BUILDPLATFORM alpine:3.15.0 AS lakectl
RUN apk add -U --no-cache ca-certificates
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build /build/lakectl ./
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

# lakefs image
FROM --platform=$BUILDPLATFORM alpine:3.15.0 AS lakefs

RUN apk add -U --no-cache ca-certificates
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

RUN mkdir -p /home/lakefs/.lakefs/plugins/ && ln -s /app/delta_diff /home/lakefs/.lakefs/plugins/delta
ENV LAKEFS_DIFF_DELTA_PLUGIN="delta"

ENTRYPOINT ["/app/lakefs"]
CMD ["run"]

