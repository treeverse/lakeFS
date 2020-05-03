FROM golang:1.14.2-alpine AS build

ARG VERSION=dev

WORKDIR /build

# Copy project deps first since they don't change often
COPY go.mod go.sum ./
RUN go mod download

# Copy project
COPY . ./

# Build a binaries
RUN go build -ldflags "-X main.Version=${VERSION}" -o lakefs
RUN go build -ldflags "-X main.Version=${VERSION}" -o lakectl ./cli

# Actual image we push
FROM alpine:3.11.5 AS run
COPY --from=build /build/lakefs /build/lakectl /go/bin/

# Setup listen port
ENV LISTEN_ADDR="0.0.0.0:4000"
EXPOSE 4000/tcp

ENTRYPOINT ["/go/bin/lakefs"]
