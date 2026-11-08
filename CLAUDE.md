# Working on ward

A [payday](https://github.com/lesomnus/payday) app. Most of it is **generated
from `proto/`**, so the usual shape of a change is: edit the schema, regenerate,
then write the part no schema can state.

`README.md` says what Ward is and how it is laid out. payday's guides are the long
version of everything below.

## Ward, specifically

- The proto package is `ward`, and payday's entities are copied into
  `proto/ward/payday/`. Ward's own RPCs go in overlays,
  `proto/ext/ward/<file>_svc.ext.proto`, and are implemented in `layer.Ops`.
- The stack is `Gate → Audit → Seal → Ops → Sink` (`cmd/serve.go`). `layer.Seal`
  refuses callers the generated writes of what only Ward writes; payday closes
  `Patch` and `Apply` at the transport.
- Only the evaluation loop writes Ward's results: `engine.Tick`, one transaction
  per tick, through `Server.Sink`. Its context carries `engine.Mark`, and
  `engine.UnlessMarked` keeps those writes off payday's trail. A write made for a
  caller uses the caller's context, so the trail shows the RPC that asked.
- No authentication yet: `cmd.Everyone` makes every method public. payday's
  default answers only health and reflection.
- A generated `Patch` needs the row's version (`date_updated`).
- After a schema change, `go run ./cmd/ward migrate plan NAME` writes the
  migration into `migrations/`. `ward serve` backs the database up, applies what
  it has not run, and refuses to start on a schema that disagrees. CI fails if a
  plan would write anything.

## Regenerate after touching the schema

```sh
go tool pd gen .          # messages, ent schema, servers, layers
go tool pd gen --check .  # what CI runs: fails if anything moved
```

A generated file that was not regenerated **compiles perfectly and is wrong**.
If you edited anything under `proto/`, you are not done until `pd gen --check`
exits 0.

```sh
go tool pd doctor .       # what would go wrong, before it does
```

## Upgrading payday

```sh
go tool pd upgrade --payday <commit> # go get, go mod tidy, then that payday's upgrade and gen
go tool pd versions .                # the page packages payday holds to a floor, and what this has
```

A commit, not `latest`: payday tags no versions. The module proxy still lists a
`v0.1.0`, tagged on 2026-10-29 and deleted since, so `latest` resolves to it --
far behind `main` -- and Go calls the move from it to a `main` pseudo-version a
"downgrade". Name a commit of `main`.

`pd upgrade` runs `go` and `npm` and **commits nothing**: read `git diff`. It
moves only what is below payday's floors, to the floor in the major it is in
(a later major only when the floor is one); keeping current beyond that is
Renovate's.

## Logging an error

```go
log.From(ctx).ErrorContext(ctx, "what failed", slog.Any("err", err))
```

The error itself, never `err.Error()`, and one to a record. The OpenTelemetry
bridge makes an error value the record's `exception.type` and
`exception.message`, turns its text into a plain string, and of two errors keeps
the last. A second failure is a record of its own, or `errors.Join`ed into the
one (payday's `docs/guide/server.md`, "Logging an error").

## Do not edit — regenerate

| | |
| --- | --- |
| `*.g.go`, `*.pb.go` | wherever `go_package` puts them |
| `server/bare/`, `server/pd/`, `internal/ent/` | in whole |
| `proto/ward/payday/` | in whole — payday's entities, **copied** in |
| `proto/**/*_svc.g.proto` | the generated contract of an entity |

**`.g` means a generator wrote it.** Everything else is yours — including
`proto/ward/*.proto`, `proto/ext/**` (overlays), `cmd/`, `cli/`, `layer/`,
`engine/` and `migrations/` (planned, then reviewed; never edited after a
database ran them).

To add a field to one of payday's entities, write an **overlay** in
`proto/ext/payday/`. Editing `proto/ward/payday/` directly is undone by the next
generation.

## Adding an entity

```sh
go tool pd entity add --global Widget .
```

Use this rather than writing the file. It picks a free domain number and writes
the tenancy out — the two things that are cheap to get wrong and expensive to
find later.

Field numbers are read by name across every entity:

- **1** key · **2** tenant · **3** *yours, a set smaller than a tenant* ·
  **4** `alias` · **5** `name` · **6** `desc` · **7** `labels` ·
  **13/14/15** the timestamps
- **8–12 and 16+** are yours. An entity that does not want 4–7 **leaves those
  numbers empty** rather than spending them on something else.

## Writing a layer

A layer embeds `Overlay` — and must also write `WithDriver`, which nothing
inherits:

```go
func (s Core) WithDriver(drv dialect.Driver) (api.Server, error) {
	next, err := enttx.Rebind(s.Next(), drv)
	if err != nil {
		return nil, err
	}

	return New(next), nil
}

var (
	_ api.Server               = Core{}
	_ enttx.Binder[api.Server] = Core{}
)
```

Leave it out and **nothing fails until a transaction is opened** — a batch, or a
multi-write RPC — because `enttx.Rebind` asks at run time. `pd doctor` finds it.

Note it is `enttx.Rebind(s.Next(), ...)` and not `s.Next().WithDriver(...)`:
`Next()` answers the generated `Server` interface, which deliberately has no
such method.

**Do not edit the generated `Gate`.** Your authorization goes in your own layer
in front of it, or into the `gate.Policy` you inject.

A rule about the row an `Add` writes goes on the server behind with
`bare.<E>WithAddRule` rather than in an `Add` you override: an entity that asks
for `AddMany` is written by both, and an override says it of one
(`docs/guide/server.md` in payday).

## Two servers, and one of them has no wall

`cmd/serve.go` builds `Walled` and `Ungated`. `Ungated` is not a privilege — it
is an instance the wall was never installed on, for work that cannot be done
from inside a tenant (`init`, resolving who is calling).

**Never hand `Ungated` to anything a caller can reach.** There is no superuser
flag to check; the wiring is the whole of the control.

## The wall is a predicate, so it only applies to reads

`Add` has no row to narrow, so it is gated in the generated `Gate` layer
instead -- and an `AddMany`, each item as an `Add`. If you are reasoning about
who may create something, that is where it is decided.

## Running

```sh
go run ./cmd/ward serve               # back up, migrate and check ward.db, then serve
go run ./cmd/ward migrate plan NAME   # after a schema change
go run ./cmd/ward config env          # every variable this can be told through
```

## `auth.Plain` is not for production

It believes what the caller writes, and it is what `serve.go` wires when
`Server.Auth` is nil. Ward asks nobody who they are yet (`cmd.Everyone`); when it
does, `Server.Auth` takes a real handler -- `auth.MTLS()`, `auth.Bearer(store)` or
`auth/authoidc` -- and `cmd.Everyone` gives way to the methods that stay public.

## Reference

- `README.md` — the same ground at length, including upgrading payday
- <https://github.com/lesomnus/payday/tree/main/docs> — the guides and the
  references behind them
