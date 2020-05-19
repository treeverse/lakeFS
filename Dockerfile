FROM golang:1.14.2-alpine AS build

ARG VERSION=dev

WORKDIR /build

# Copy project deps first since they don't change often
COPY go.mod go.sum ./
RUN go mod download

# Copy project
COPY . ./

# Build a binaries
RUN go build -ldflags "-X github.com/treeverse/lakefs/config.Version=${VERSION}" -o lakefs ./cmd/lakefs
RUN go build -ldflags "-X github.com/treeverse/lakefs/config.Version=${VERSION}" -o lakectl ./cmd/lakectl

# lakectl image
FROM alpine:3.11.5 AS lakectl
WORKDIR /app
ENV PATH /app:$PATH
COPY --from=build /build/lakectl ./
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs
ENTRYPOINT ["/app/lakectl"]

# lakefs image
FROM alpine:3.11.5 AS lakefs

# Be Docker compose friendly (i.e. support wait-for)
RUN apk add netcat-openbsd

WORKDIR /app
COPY ./wait-for ./
ENV PATH /app:$PATH
COPY --from=build /build/lakefs /build/lakectl ./

EXPOSE 8000/tcp
EXPOSE 8001/tcp

# Setup user
RUN addgroup -S lakefs && adduser -S lakefs -G lakefs
USER lakefs
WORKDIR /home/lakefs

# Configuration location
VOLUME /etc/lakefs.yaml

ENTRYPOINT ["/app/lakefs"]
