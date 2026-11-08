# ward

Ward reads OpenTelemetry signals, applies rules to them, and answers over gRPC
whether there is a problem, whether it was resolved, or whether that cannot be
known right now.

It is built on [payday](https://github.com/lesomnus/payday). The implementation
plan is [#21](https://github.com/lesomnus/ward/issues/21).

## Status

The skeleton ([#22](https://github.com/lesomnus/ward/issues/22)): the `Issue`
entity on SQLite, versioned migrations, the payday stack Ward runs on, and the
write path of the evaluation loop. No rule is evaluated yet.

Nothing is authenticated yet: every RPC is open to every caller.

## Layout

| | |
| --- | --- |
| `proto/ward/` | the schema. `*_svc.g.proto` and `payday/` are generated |
| `layer/` | Ward's layers of the payday stack: `Seal` refuses callers the writes only Ward makes, `Ops` holds Ward's own RPCs |
| `engine/` | the evaluation loop: the transaction a tick commits in, and the filter that keeps its writes off payday's trail |
| `migrations/` | SQL planned from the schema, reviewed, and embedded in the binary |
| `cli/`, `cmd/` | the `ward` binary and its wiring |

The stack a call goes down: `Gate → Audit → Seal → Ops → Sink`. The
evaluation loop writes through the sink, under Ward's layers.

## Working on it

```sh
go tool pd gen .                      # after editing proto/
go run ./cmd/ward migrate plan NAME   # after a schema change: write the migration, then read it
go test ./...
go run ./cmd/ward serve               # back up, migrate and check ward.db, then serve on :14319
```

`ward.yaml` is the configuration, every value written out.

CI runs `pd gen --check`, `pd doctor`, a migration plan that has to come out
empty, `go vet` and the tests.
