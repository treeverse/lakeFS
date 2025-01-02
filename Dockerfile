# syntax=docker/dockerfile:1
ARG VERSION=dev

ARG BUILD_REPO=golang
ARG BUILD_TAG=1.22.6-alpine3.20
ARG BUILD_PACKAGES="build-base ca-certificates"

ARG IMAGE_REPO=alpine
ARG IMAGE_TAG=3.18
ARG IMAGE_PACKAGES=ca-certificates

ARG ADD_PACKAGES="apk add -U --no-cache"


FROM --platform=$BUILDPLATFORM $BUILD_REPO:$BUILD_TAG AS build
ARG ADD_PACKAGES BUILD_PACKAGES

WORKDIR /build
RUN $ADD_PACKAGES $BUILD_PACKAGES
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg go mod download
COPY . ./

FROM build AS build-lakefs
ARG VERSION TARGETOS TARGETARCH ADD_PACKAGES BUILD_PACKAGES
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakefs ./cmd/lakefs

FROM build AS build-lakectl
ARG VERSION TARGETOS TARGETARCH ADD_PACKAGES BUILD_PACKAGES
RUN --mount=type=cache,target=/root/.cache/go-build \
    --mount=type=cache,target=/go/pkg \
    GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -ldflags "-X github.com/treeverse/lakefs/pkg/version.Version=${VERSION}" -o lakectl ./cmd/lakectl

FROM $IMAGE_REPO:$IMAGE_TAG AS lakectl
ARG ADD_PACKAGES IMAGE_PACKAGES
WORKDIR /app
ENV PATH=/app:$PATH
COPY --from=build-lakectl /build/lakectl /app/
RUN $ADD_PACKAGES $IMAGE_PACKAGES
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
