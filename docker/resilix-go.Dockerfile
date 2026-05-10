FROM golang:1.24-alpine AS builder
WORKDIR /src
COPY . .
RUN cd sdk/go && go build -o /out/resilixctl ./cmd/resilixctl

FROM alpine:3.21
RUN adduser -D -u 10001 resilix
USER resilix
COPY --from=builder /out/resilixctl /usr/local/bin/resilixctl
ENTRYPOINT ["/usr/local/bin/resilixctl"]
