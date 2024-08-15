# syntax=docker/dockerfile:1
ARG VERSION=dev

FROM --platform=$BUILDPLATFORM golang:1.22.6-alpine3.20 AS build
WORKDIR /build
RUN apk add --no-cache build-base ca-certificates
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg go mod download
COPY . ./

FROM build as build-lakefs
ARG VERSION TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakefs ./cmd/lakefs

FROM build as build-lakectl
ARG VERSION TARGETOS TARGETARCH
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakectl ./cmd/lakectl

FROM alpine:3.18 AS lakectl
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build-lakectl /build/lakectl /app/
RUN apk add -U --no-cache ca-certificates
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

FROM lakectl AS lakefs
COPY ./scripts/wait-for /app/
COPY --from=build-lakefs /build/lakefs /app/
EXPOSE 8000/tcp
ENTRYPOINT ["/app/lakefs"]
CMD ["run"]
