# emitter

A service that sends signals to Ward, for the local environment
(`deploy/compose`). It sends logs and metrics with the OpenTelemetry Go SDK to
its node's Collector, and does what it is told over HTTP while it runs.

```sh
OTEL_SERVICE_NAME=emitter OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317 \
  go run ./example/emitter -control :8080
```

## What it always sends

- **Resource**: `service.name` from `OTEL_SERVICE_NAME` (and anything in
  `OTEL_RESOURCE_ATTRIBUTES`), and a random `service.instance.id`, new on every
  start. Not `host.name`: the node's Collector sets that.
- **Liveness counter** (`-liveness`, `emitter.frames` by default): a cumulative
  monotonic Sum, `Add(0)` at start and one more per run of the main loop
  (`-tick`, 100 ms). Its StartTimeUnixNano is when this process started.
- Metrics are exported every second (`-export-interval`), cumulative; logs in
  batches every second (`-log-interval`).

| Flag | Variable | Default |
| --- | --- | --- |
| `-control` | `EMITTER_CONTROL` | `:8080` |
| `-liveness` | `EMITTER_LIVENESS` | `emitter.frames` |
| `-tick` | `EMITTER_TICK` | `100ms` |
| `-export-interval` | `EMITTER_EXPORT_INTERVAL` | `1s` |
| `-log-interval` | `EMITTER_LOG_INTERVAL` | `1s` |

The exporters read the standard variables: `OTEL_EXPORTER_OTLP_ENDPOINT` (an
`http://` endpoint is a plain-text connection), `OTEL_SERVICE_NAME`,
`OTEL_RESOURCE_ATTRIBUTES`.

## Control

Every command is a `POST` with its arguments in the query, and answers with one
line saying what it did (also written to the emitter's stderr, so `docker
compose logs` tells the story). `GET /` answers with everything it is doing,
as JSON. `msg=` sets the body of any log record.

```sh
curl -X POST 'localhost:18081/condition/repeat?type=emitter.overheat&key=left&every=500ms'
curl localhost:18081/
```

### Logs

| Command | Sends |
| --- | --- |
| `/condition/repeat?type=T&key=K&every=500ms` | ERROR with `diag.condition.type=T`, `diag.condition.key=K`, every 500 ms until stopped (`every` is a duration, or a number of milliseconds; 1s by default). Again for the same T and K replaces it |
| `/condition/stop?type=T&key=K` | stops that one; without `type`, every repeating condition |
| `/condition/once?type=T&key=K` | one such ERROR |
| `/condition/release?type=T&key=K` | INFO with the same type and key and `diag.condition.active=false`, a bool: the release |
| `/condition/release?type=T&key=K&as=string` | the same with `"false"` as a **string**: a type violation, which a rule asking for the bool must not take as a release |
| `/error/repeat?every=1s` | a plain ERROR, no attributes, every second until stopped |
| `/error/once` | one plain ERROR |
| `/error/stop` | stops the plain ERROR |
| `/error/once?error_type=timeout&server_address=db:5432` | an ERROR carrying only standard attributes, `error.type` (and `server.address`); `/error/repeat` takes them too |
| `/silence`, `/silence/stop` | no log record at all while silent (one-shots answer 409); metrics go on |

`key` may be left out: the record then has no `diag.condition.key`, which a rule
keyed on it reads as `""`. Without `msg=`, an ERROR whose body says "timed out"
is what the Collector's `transform/diag` turns into `error.type=timeout`:
`/error/once?msg=upstream+timed+out`.

### The process

| Command | Does |
| --- | --- |
| `/hang`, `/hang/stop` | the main loop stops working: the liveness and message counters stop growing. The SDK still exports their flat cumulative values, so data points keep arriving. To stop the data points too, `docker compose pause` the container |
| `/exit?code=1&after=1s` | exits with `code` after `after`, without flushing, as a crash would. Under compose (`restart: unless-stopped`) it comes back with a new `service.instance.id` and a new start time on its counters: a source restart |

### Metrics

| Command | Sends |
| --- | --- |
| `/gauge?name=battery.charge&value=15&attr.battery.id=main` | a gauge called `name`, made the first time it is named, at `value`; each `attr.NAME=VALUE` is a string attribute of the series |
| `/gauge/stop?name=battery.charge` | the gauge's series stop: no data points, which is a gauge gone rather than one at 0 |
| `/messages?stream=main&rate=10` | `emitter.messages{stream=main}`, a cumulative counter, growing by 10 per second; `rate=0` keeps the series flat |
| `/hw?id=disk0&state=degraded&type=physical_disk` | `hw.status`: a gauge with one series per `hw.state` (`ok`, `degraded`, `failed`, `needs_cleaning`, `predicted_failure`, and `state` if it is none of those), 1 on the current state and 0 on the rest, with `hw.id` and `hw.type` (`physical_disk` by default). OpenTelemetry's hardware conventions make `hw.status` an UpDownCounter; the shape of the series is the same |
| `/hw/stop?id=disk0` | that device's series stop; without `id`, every device's |
