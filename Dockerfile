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

# Actual image we push
FROM alpine:3.11.5 AS run

# Be Docker compose friendly (i.e. support wait-for)
RUN apk add netcat-openbsd

WORKDIR /app
COPY ./wait-for ./
ENV PATH /app:$PATH
COPY --from=build /build/lakefs /build/lakectl ./

EXPOSE 8000/tcp
EXPOSE 8001/tcp

# Setup user
RUN addgroup -S treeverse && adduser -S treeverse -G treeverse
USER treeverse
WORKDIR /home/treeverse

# Configuration location
VOLUME /etc/lakefs.yaml

ENTRYPOINT ["/app/lakefs"]
