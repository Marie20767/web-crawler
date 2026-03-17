# =========================
# Stage 1: Build the binary
# =========================
FROM golang:1.25 AS builder

WORKDIR /app

COPY go.mod go.sum Makefile ./
RUN make dep

COPY . .

RUN make build

# =========================
# STAGE 2: runtime
# =========================
FROM alpine:3.22

RUN addgroup -g 1000 -S producer && adduser -u 1000 -S producer -G producer

RUN mkdir /app && chown producer:producer /app
WORKDIR /app

COPY --from=builder /app/go-url-producer /app/go-url-producer

USER producer

ENTRYPOINT ["/app/go-url-producer"]