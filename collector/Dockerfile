# A Collector with Ward in it, built by ocb from collector/builder-config.yaml.
# From the repository's root:
#
#   docker build -f collector/Dockerfile -t ward-collector:local .
FROM golang:1.27.2-bookworm AS build
ENV CGO_ENABLED=0 GOTOOLCHAIN=local
WORKDIR /src
COPY . .
WORKDIR /src/collector
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go run go.opentelemetry.io/collector/cmd/builder@v0.165.0 --config builder-config.yaml \
 && mkdir /out && cp build/otelcol-ward /out/ && mkdir /out/state

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/otelcol-ward /usr/local/bin/otelcol-ward
COPY --from=build --chown=65532:65532 /out/state /var/lib/ward
USER 65532:65532
WORKDIR /var/lib/ward
EXPOSE 14319 14317 14318
ENTRYPOINT ["otelcol-ward"]
CMD ["--config", "/etc/otelcol-ward/config.yaml"]
