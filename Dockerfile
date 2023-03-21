FROM --platform=$BUILDPLATFORM golang:1.19.2-alpine3.16 AS build

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
FROM --platform=$BUILDPLATFORM rust:1.68-alpine3.16 AS build-delta-diff-plugin
#RUN apk add build-base && apk add pkgconfig && apk add libressl-dev

ENV GLIBC_REPO=https://github.com/sgerrand/alpine-pkg-glibc
ENV GLIBC_VERSION=2.35-r0
RUN set -ex
RUN apk --force-overwrite add libstdc++ curl ca-certificates
RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
RUN curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk -o /tmp/glibc-${GLIBC_VERSION}.apk
RUN curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/glibc-bin-${GLIBC_VERSION}.apk -o /tmp/glibc-bin-${GLIBC_VERSION}.apk
RUN apk add --force-overwrite --allow-untrusted /tmp/*.apk
RUN /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib
RUN apk add --force-overwrite --no-cache musl-dev
RUN apk add libressl-dev

RUN cargo new --bin delta-diff
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
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS lakectl
RUN apk add -U --no-cache ca-certificates
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build /build/lakectl ./
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

# lakefs image
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

# lakefs image
FROM --platform=$BUILDPLATFORM alpine:3.16.0 AS lakefs-plugins

RUN apk update

#ENV GLIBC_REPO=https://github.com/sgerrand/alpine-pkg-glibc
#ENV GLIBC_VERSION=2.35-r0
#RUN set -ex
#RUN apk --force-overwrite add libstdc++ curl ca-certificates
#RUN wget -q -O /etc/apk/keys/sgerrand.rsa.pub https://alpine-pkgs.sgerrand.com/sgerrand.rsa.pub
#RUN curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/glibc-${GLIBC_VERSION}.apk -o /tmp/glibc-${GLIBC_VERSION}.apk
#RUN curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/glibc-bin-${GLIBC_VERSION}.apk -o /tmp/glibc-bin-${GLIBC_VERSION}.apk
#RUN apk add --force-overwrite --allow-untrusted /tmp/*.apk
#RUN /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib

#RUN for pkg in glibc-${GLIBC_VERSION} glibc-bin-${GLIBC_VERSION}; \
#        do curl -sSL ${GLIBC_REPO}/releases/download/${GLIBC_VERSION}/${pkg}.apk -o /tmp/${pkg}.apk; done && \
#    apk add --force-overwrite --allow-untrusted /tmp/*.apk && \
#    rm -v /tmp/*.apk && \
#    /usr/glibc-compat/sbin/ldconfig /lib /usr/glibc-compat/lib \

RUN apk add libressl-dev && apk add libc6-compat && apk add libgcc
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

RUN mkdir -p /home/lakefs/.lakefs/plugins/diff && ln -s /app/delta_diff /home/lakefs/.lakefs/plugins/diff/delta

ENTRYPOINT ["/app/lakefs"]
CMD ["run"]

