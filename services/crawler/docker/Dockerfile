# =========================
# Stage 1: Build the binary
# =========================
FROM golang:1.25 AS builder

WORKDIR /app

COPY shared/ /shared/

COPY services/crawler/go.mod services/crawler/go.sum services/crawler/Makefile ./
RUN make dep

COPY services/crawler/ .

RUN make build

# =========================
# STAGE 2: runtime
# =========================
FROM alpine:3.22

RUN addgroup -g 1000 -S crawler && adduser -u 1000 -S crawler -G crawler

RUN mkdir /app && chown crawler:crawler /app
WORKDIR /app

COPY --from=builder /app/url-crawler /app/url-crawler

USER crawler

ENTRYPOINT ["/app/url-crawler"]