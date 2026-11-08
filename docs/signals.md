# Sending signals Ward can judge

Ward judges what services already send through OpenTelemetry: log records and
metric data points, over OTLP, through each node's Collector. There is no Ward
library, endpoint or protocol. This page is what a service does so that rules
can say more about it -- above all, so that Ward can tell a problem that ended
from one it stopped hearing about.

Everything here is a convention, not something Ward requires. A plain ERROR log
is enough for a rule (`match: {severity: {gte: error}}`); the rest makes the
Conditions finer and their resolution provable.

## `service.name` is the source

A source is one `service.name`. Use a fixed value per service, and one process
per `service.name` on a node: Ward reads a change of process as a restart.
`service.instance.id` may be new on every start, as OpenTelemetry recommends.

## A liveness counter

Ward resolves a repeating problem by silence only when it has evidence that the
source was alive and sending after the silence: a data point of a counter the
source names in its definition.

```yaml
sources:
  detector:
    liveness: {metric: detector.frames, max_gap: 3s}
```

- A **cumulative** counter (an OTel `Counter`, the SDK's default temporality).
  A cumulative Sum is exported on every interval even when it did not grow, so
  data points stop only when the process or its export stops.
- Created at start: `Add(0)` once, so the series exists before anything happens.
- Exported often: `max_gap` is a few export intervals. The Go SDK's default
  interval is 60 s; set `OTEL_METRIC_EXPORT_INTERVAL` (milliseconds), e.g. 1000.
- Any counter the service already has will do -- frames processed, messages
  sent. Ward reads only whether points keep coming, and their
  `StartTimeUnixNano`: a new start time, or a new `service.instance.id`, is a
  restart.
- Incremented from the main loop rather than from a side thread, so that a hung
  process shows as a flat counter (a `rate` rule) and not only a dead one.

A source with no such counter is `liveness: none`. Its repeating problems close
as `quiet` -- not resolved -- since nothing says it was alive to report them.

```go
frames, _ := otel.Meter("detector").Int64Counter("detector.frames")
frames.Add(ctx, 0)
for {
	frames.Add(ctx, 1)
	// one iteration of the work
}
```

## Log records

Rules match records by severity, `EventName`, attributes and resource
attributes. Keep the standard fields standard:

| Field | Use |
| --- | --- |
| `Timestamp` | when it happened; every time a rule computes is from it |
| `SeverityNumber` | WARN is 13, ERROR 17 |
| `Body` | for people. Rules do not read it |
| `error.type`, `exception.*` | what went wrong |
| `server.address` and other semantic conventions | what it went wrong with |

### The `diag.*` attributes

Three attributes make a Condition easy to name. Ward treats them like any other
attribute; rules point at them by name.

| Attribute | Type | Means |
| --- | --- | --- |
| `diag.condition.type` | string | the kind of problem, e.g. `detector.inference_failed` |
| `diag.condition.key` | string | which one of its kind, e.g. `camera.front` |
| `diag.condition.active` | **bool** | `false` on the record that says it ended |

```go
// Repeating: logged every time it happens, while it lasts.
logger.LogAttrs(ctx, slog.LevelError, "inference failed",
	slog.String("diag.condition.type", "detector.inference_failed"),
	slog.String("diag.condition.key", "camera.front"))

// It ended (optional, and needed for a problem that is reported once).
logger.LogAttrs(ctx, slog.LevelInfo, "inference recovered",
	slog.String("diag.condition.type", "detector.inference_failed"),
	slog.String("diag.condition.key", "camera.front"),
	slog.Bool("diag.condition.active", false))
```

and the rule that reads them:

```yaml
- id: detector.inference_failed
  kind: log
  source: detector
  severity: error
  match: {attributes: {diag.condition.type: detector.inference_failed}}
  key: [{attribute: diag.condition.key}]
  activate: {count: 3, within: 200ms}
  resolve_after: 300ms
  release: {attributes: {diag.condition.type: detector.inference_failed, diag.condition.active: false}}
  on_source_restart: close
  operator: {ack: required, latch: false}
```

Types count. `release` above compares with the bool `false`; the string
`"false"` does not match it, and Ward reports the record under
`StatusService/Get` as a type violation. Send an attribute with one type, always,
and as a flat dotted key -- not as a nested group of the same path.

### Repeating or once

| The problem is reported | Rule | Ends when |
| --- | --- | --- |
| over and over while it lasts (each failed frame, each failed retry) | `resolve_after: D` | nothing came for `D` while the source was alive -- `resolved.silence` |
| once, when it starts | `forget_after: D` and a `release` | the release record came -- `resolved.explicit`; or `D` went by -- `quiet`, not resolved |

A repeating report is what lets Ward resolve a problem on its own, so prefer to
log on every occurrence rather than once. Do not throttle below a rule's
`resolve_after`: a report every 10 s looks like silence to a rule that waits 5.

## Without `diag.*`

Rules need none of the above to make Conditions:

- **Standard attributes**: `match: {severity: {gte: error}, attributes: {error.type: timeout}}`
  with `key: [{attribute: server.address}]`.
- **Metrics**: any gauge or counter the service exports, by name and attributes
  (`hw.status` and its `hw.state`, `system.filesystem.utilization`), judged in
  levels or as a discrete state. A counter is read as a `rate`; one that stops is
  `points` below 1; one that keeps starting over is `resets`.
- **The body**, only by way of the Collector: turn what a message says into an
  attribute there, and match the attribute.

```yaml
processors:
  transform/diag:
    log_statements:
      - context: log
        statements:
          - set(attributes["error.type"], "timeout") where attributes["error.type"] == nil and IsMatch(body, "(?i)timed? ?out")
```

`deploy/collector/collector.yaml` is a node's full diag pipeline, and
`example/emitter` is a service that sends all of the above on command.
[conditions.md](conditions.md) goes through every kind of alarm a rule can
make -- the signals each expects, a scenario and a rule -- in Korean.
