# syntax=docker/dockerfile:1

# Ward, built static and run as nonroot (uid 65532) on distroless.
#
#   docker build -t ward .
#   docker run --rm ward version
#   docker run --rm -v ./ward.yaml:/etc/ward/ward.yaml:ro -v ward:/var/lib/ward ward
#
# The configuration is read from /etc/ward/ward.yaml. /var/lib/ward is the
# working directory and belongs to the user Ward runs as: mount a volume there
# and point `db.dsn` into it (deploy/compose/ward.yaml does). The migrations are
# embedded in the binary; `serve` applies them, backing the database up beside
# itself first.
#
# The repository is not copied in (.dockerignore), so the binary carries no
# revision; name the release with --build-arg VERSION=$(git describe --always --dirty).

FROM golang:1.27.4-bookworm AS build

ARG VERSION=""
ENV CGO_ENABLED=0 GOTOOLCHAIN=local

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath \
        -ldflags "-s -w -X github.com/lesomnus/payday/version.version=${VERSION}" \
        -o /out/ward ./cmd/ward \
 && mkdir /out/state

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/ward /usr/local/bin/ward
COPY --from=build --chown=65532:65532 /out/state /var/lib/ward

USER 65532:65532
WORKDIR /var/lib/ward

# The gRPC API, and the OTLP receiver over gRPC and HTTP.
EXPOSE 14319 14317 14318

ENTRYPOINT ["ward"]
CMD ["--config", "/etc/ward/ward.yaml", "serve"]
