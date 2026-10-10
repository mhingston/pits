# Sandbox SDK 1.0 requires the matching sandbox-shim version.
FROM docker.io/cloudflare/sandbox:1.0.0 AS shim
FROM alpine:3.23
RUN apk add --no-cache bash git ca-certificates coreutils util-linux procps
COPY --from=shim /usr/local/bin/sandbox-shim /usr/local/bin/sandbox-shim
RUN mkdir -p /workspace /var/lib/pits-processes
WORKDIR /workspace
CMD ["sleep", "infinity"]
