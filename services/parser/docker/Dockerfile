# =========================
# Stage 1: Build the binary
# =========================
FROM golang:1.25 AS builder

WORKDIR /app

COPY shared/ /shared/

COPY services/parser/go.mod services/parser/go.sum services/parser/Makefile ./
RUN make dep

COPY services/parser/ .

RUN make build

# =========================
# STAGE 2: runtime
# =========================
FROM alpine:3.22

RUN addgroup -g 1000 -S parser && adduser -u 1000 -S parser -G parser

RUN mkdir /app && chown parser:parser /app
WORKDIR /app

COPY --from=builder /app/url-parser /app/url-parser

USER parser

ENTRYPOINT ["/app/url-parser"]