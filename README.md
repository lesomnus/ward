# ward

Ward reads OpenTelemetry signals, applies rules to them, and answers over gRPC
whether there is a problem, whether it was resolved, or whether that cannot be
known right now.

It is built on [payday](https://github.com/lesomnus/payday). The implementation
plan is [#21](https://github.com/lesomnus/ward/issues/21).

## Status

A first prototype: everything but the Collector exporter adapter
([#41](https://github.com/lesomnus/ward/issues/41)) runs, and runs together in
`deploy/compose`.

- **Signals in**: OTLP over gRPC and HTTP (gzip), with a bounded queue that
  tells a Collector to retry when it is full.
- **Definitions in**: sources, rules and components from provider files,
  re-read when they change; a broken file keeps its last good revision.
- **Judgement**: log, metric (`points`, `resets`, `rate`, `last`, `avg`,
  `min`, `max`, `change`; levels, deadband, `relative_to`, discrete states),
  node and composite rules, three-valued suppression, the silence decision table
  with liveness evidence, restarts and reboots, the node gate from canaries,
  latching, key limits, and the operators' acknowledge, comment, reset, shelve
  and out-of-service.
- **Out**: Issues and their transitions in SQLite, the gRPC API below, the
  `Follow` change feed, and optionally Issue transitions and Ward's own metrics
  over OTLP.

Nothing is authenticated: every RPC is open to every caller.

## Running it

```sh
cd deploy/compose && docker compose up -d --build   # two nodes' Collectors, emitters and Ward
go test -tags e2e -count=1 -v ./deploy/compose/      # the scenarios, against it
```

`deploy/README.md` has the emitters' commands and how to inject faults.
`go run ./cmd/ward serve` runs Ward alone on the repository's `ward.yaml`, which
expects a Collector on this computer that names its node `local`.

## API

gRPC on `:14319`, every service in package `ward`:

| Service | |
| --- | --- |
| `IssueService` | `Get`, `List` (by state, rule, node, attention), `Search` (by when it opened and closed); `Acknowledge`, `Comment`, `Reset` |
| `IssueEventService` | `Get`, `List`; `Follow`, the change feed, from a cursor |
| `ConditionService` | the current state of every (rule, node, key) -- normal ones too, with the value last judged -- and of the ones a Component expects before any signal of them (`unobserved`); `Shelve`, `Unshelve`, `RemoveFromService`, `PlaceInService` |
| `SourceService`, `RuleService`, `ProviderService` | what is defined, by whom, and what each source is doing on each node |
| `NodeService` | whether each node is observable, and why not |
| `StatusService` | Ward itself: receiver queue, late signals, records no rule reads, attributes of the wrong type, unknown hosts, silent sources |
| `ComponentService` | the component tree: each component's health, the kinds of alarm that can open under it, and its Conditions -- an inventory, when its members name rule, node and Key |

## Layout

| | |
| --- | --- |
| `proto/ward/` | the schema. `*_svc.g.proto` and `payday/` are generated |
| `conf/` | Ward's blocks of the static configuration, and their validation |
| `def/` | what a provider supplies: parsing, validation, merging providers |
| `provider/` | the file provider and the registry that merges them |
| `receiver/otlp/` | OTLP over gRPC and HTTP into a `signal.Sink` |
| `engine/` | the normalizer, the evaluator, the node gate and canary, the Store, and the loop that ties them |
| `api/` | the services that answer from the loop's state |
| `exporter/` | Ward's own records over OTLP |
| `layer/` | Ward's layers of the payday stack: `Seal` refuses callers the writes only Ward makes, `Ops` holds Ward's own RPCs |
| `migrations/` | SQL planned from the schema, reviewed, and embedded in the binary |
| `cli/`, `cmd/` | the `ward` binary and its wiring |
| `example/` | the emitter that plays a service, an example provider file, and `robot/`: a robot's programs, the signals and attributes each sends, the rules, and a simulation of what Ward concludes (in Korean) |
| `collector/` | the ward exporter: Ward inside an OpenTelemetry Collector, and how to add it to a distribution (`collector/README.md`) |
| `deploy/` | a node's Collector configuration, and the compose environment |
| `docs/` | `signals.md`, what a service sends; `conditions.md`, the kinds of alarm a rule makes, with the signals, scenarios and configuration of each (in Korean) |

The stack a call goes down: `Gate → Audit → Seal → Ops → Sink`. The
evaluation loop writes through the sink, under Ward's layers, one transaction a
tick.

## Working on it

```sh
go tool pd gen .                      # after editing proto/
go run ./cmd/ward migrate plan NAME   # after a schema change: write the migration, then read it
go test ./...
go run ./cmd/ward serve               # back up, migrate and check ward.db, then serve
```

`ward.yaml` is the configuration, every value written out; Ward refuses to
start on one left out.

CI runs `pd gen --check`, `pd doctor`, a migration plan that has to come out
empty, `go vet` and the tests.
