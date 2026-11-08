# syntax=docker/dockerfile:1

# The emitter, built from the repository root (it is a package of Ward's
# module):
#
#   docker build -f example/emitter/Dockerfile -t ward-emitter .

FROM golang:1.27.2-bookworm AS build

ENV CGO_ENABLED=0 GOTOOLCHAIN=local

WORKDIR /src
COPY . .
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go build -trimpath -ldflags "-s -w" -o /out/emitter ./example/emitter

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/emitter /usr/local/bin/emitter

USER 65532:65532

# The control endpoint.
EXPOSE 8080

ENTRYPOINT ["emitter"]
