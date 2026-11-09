# The ward exporter

Ward as an OpenTelemetry Collector exporter (design §8.4). Built into a
Collector, it takes what the Collector's pipelines end with, evaluates it, and
serves Ward's API from inside the Collector's process.

| | |
| --- | --- |
| `wardexporter/` | the exporter, a Go module of its own |
| `builder-config.yaml`, `Dockerfile` | a Collector with Ward in it, built from this checkout (`deploy/compose/docker-compose.collector.yaml` runs it) |

## Adding it to a Collector distribution

In the distribution's builder manifest:

```yaml
exporters:
  - gomod: github.com/lesomnus/ward/collector/wardexporter <version>
```

Ward tags no versions; the version is a commit of `main`. This prints the latest:

```sh
go list -m github.com/lesomnus/ward/collector/wardexporter@main
```

The exporter's `go.mod` names the Ward it was built with. To take a later Ward
than that, add it to the manifest's `replaces`:

```yaml
replaces:
  - github.com/lesomnus/ward => github.com/lesomnus/ward <version>   # go list -m github.com/lesomnus/ward@main
```

What the distribution needs:

- **Collector v0.163.0 or later** (core modules v1.69.0). Ward and the libraries
  under it log through OpenTelemetry Go's stable logs API
  (`go.opentelemetry.io/otel/log` v1); the Collector moved to it in v0.163.0.
  Built into an older Collector, Go picks the stable API for the Collector's own
  pre-1.0 log bridges (`otelzap`, `otlploghttp`), and they do not compile. A
  distribution on a later Collector keeps its own versions.
- **Go 1.27 or later** to build.
- No cgo: Ward's SQLite driver is pure Go.

## Configuring it

The exporter's block holds the queue size and Ward's static configuration --
what a `ward.yaml` holds -- without `receivers`: the Collector's own receivers
deliver the signals. Nothing has a default; a value left out is refused when the
Collector starts. `deploy/compose/collector-ward.yaml` is a complete example.

```yaml
exporters:
  ward:
    queue_size: 4096        # hand-overs held before what comes next is dropped
    ward:
      server: {addr: ":14319"}             # Ward's gRPC API
      db: {driver: sqlite3, dsn: "file:/var/lib/ward/ward.db?..."}
      watch: {broker: none}
      spin: {lease: local}
      audit: {retain: 8760h, discard: true}
      operator: {max_shelve: 8h}
      engine: {...}
      nodes:
        robot-1: {match: {host.name: robot-1}, collector: "127.0.0.1:4317"}
      providers:
        - {name: rules, type: file, path: /etc/ward/rules.yaml, poll_interval: 10s, debounce: 200ms}
      store: {retention: 720h}
      exporters: []

service:
  pipelines:
    logs:    {receivers: [otlp], processors: [resource/node, batch], exporters: [ward]}
    metrics: {receivers: [otlp, host_metrics], processors: [resource/node, batch], exporters: [ward]}
```

- **One Ward per name.** A logs and a metrics exporter named `ward` are the same Ward.
  It starts with the first and stops with the last.
- **It never fails the pipeline.** It queues what it is handed and returns at
  once: an exporter's error can travel back to the apps that sent the signal.
  When the queue is full it drops and counts (`StatusService`), and the canaries
  dropped with it make the node's gate fail, so nothing is judged on the gap.
- **Ward inside a node's own Collector.** For one robot or one host:
  - the node's `collector`, where Ward sends its canaries, is this Collector's
    own OTLP gRPC receiver;
  - `host.name` is set by a processor ahead of the exporter, since the node's
    `match` reads it;
  - `host_metrics` with the `system` scraper gives Ward the boot time (`hostmetrics`,
    the name before v0.163.0, still loads as a deprecated alias).
- **The process.** Ward's API listens on `ward.server.addr`. The database at
  `ward.db.dsn` has to be writable. Ward logs through the Collector's logger.
