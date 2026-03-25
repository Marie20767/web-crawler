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

RUN addgroup -g 1000 -S initialiser && adduser -u 1000 -S initialiser -G initialiser

RUN mkdir /app && chown initialiser:initialiser /app
WORKDIR /app

COPY --from=builder /app/url-initialiser /app/url-initialiser

USER initialiser

ENTRYPOINT ["/app/url-initialiser"]