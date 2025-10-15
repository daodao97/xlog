FROM golang:1.25 AS builder

ARG TARGETOS=linux
ARG TARGETARCH=amd64

WORKDIR /src

COPY go.mod ./

RUN apt-get update \
	&& apt-get install -y --no-install-recommends build-essential cmake git \
	&& rm -rf /var/lib/apt/lists/*

RUN go mod download

COPY . .

RUN CGO_ENABLED=1 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -o /bin/xlog ./cmd/xlog

FROM ubuntu:24.04

RUN apt-get update \
	&& apt-get install -y --no-install-recommends ca-certificates libstdc++6 \
	&& rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY --from=builder /bin/xlog /usr/local/bin/xlog

ENV XLOG_HTTP_ADDR=:8080
ENV XLOG_DUCKDB_PATH=/data/logs.duckdb
ENV XLOG_LOG_TAIL=200
ENV XLOG_LOG_SINCE=15m

VOLUME ["/data"]

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/xlog"]
