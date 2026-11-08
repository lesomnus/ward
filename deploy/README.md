# Running Ward locally

`compose/` is a small deployment on one machine: two nodes, each an
OpenTelemetry Collector with services sending to it, and Ward receiving from
both.

```
  emitter-1 ──▶ edge-1 (Collector) ──┐
                                     ├──▶ ward ── gRPC API :14319
  emitter-2 ──▶ edge-2 (Collector) ──┘
  reporter  ──┘
```

| | |
| --- | --- |
| `collector/collector.yaml` | the Collector of one node: the diag pipelines, which pass everything but Ward's own signals to Ward |
| `compose/docker-compose.yaml` | the environment |
| `compose/ward.yaml` | Ward's configuration, every value written out |
| `compose/providers/` | the sources and rules, mounted into Ward; edit them while it runs |
| `../example/emitter/` | the service the emitters run, and its control endpoint |
| `../Dockerfile` | Ward's image |
| `compose/docker-compose.collector.yaml`, `compose/collector-ward.yaml` | the same environment with Ward inside a Collector |

`compose/compose_test.go` checks the configuration the way Ward reads it:
`ward.yaml` complete, every provider file parsed and merged, the nodes matching
the Collectors. `go test ./...` runs it.

## Up and down

```sh
cd deploy/compose
docker compose up -d --build
docker compose ps
docker compose logs -f ward
docker compose down -v          # -v removes Ward's database too
```

| Service | Node | `service.name` | Published |
| --- | --- | --- | --- |
| `ward` | | | 14319 gRPC API, 14317 OTLP/gRPC, 14318 OTLP/HTTP |
| `edge-1` | edge-1 | (Collector) | |
| `edge-2` | edge-2 | (Collector) | |
| `emitter-1` | edge-1 | `emitter` | 18081 control |
| `emitter-2` | edge-2 | `emitter` | 18082 control |
| `reporter` | edge-2 | `reporter` | 18083 control |

Ward is started as `ward --config /etc/ward/ward.yaml serve`, with its SQLite
database in the volume `ward-state` at `/var/lib/ward`. Each Collector sends to
`http://ward:14318`; Ward sends its canary to each Collector's `:4317`.

The Collectors and emitters run without Ward, for working on the pipeline or
on Ward outside the container:

```sh
docker compose up -d --build --scale ward=0
```

To see what a Collector passes on, add a `debug` exporter to its pipelines with
a second configuration file (the Collector merges them); keep it out of the
repository:

```yaml
# debug.yaml, mounted beside collector.yaml and named by a second --config=
exporters:
  debug: {verbosity: detailed, sampling_initial: 1000000, sampling_thereafter: 1}
service:
  pipelines:
    logs/diag: {exporters: [otlp_http/diag, debug]}
    metrics/diag: {exporters: [otlp_http/diag, debug]}
```

Check a Collector configuration with the image itself:

```sh
docker run --rm -v ./deploy/collector/collector.yaml:/c.yaml:ro \
  -e WARD_OTLP_HTTP_ENDPOINT=http://ward:14318 -e NODE_HOST_NAME=edge-1 \
  otel/opentelemetry-collector-contrib:0.163.2 validate --config=/c.yaml
```

## Making things happen

The emitters are told what to send over HTTP; `example/emitter/README.md` has
every command. The rules in `compose/providers/` each say which command makes
them fire. Some:

```sh
E1=localhost:18081

# A repeating condition: active after three records within 2 s, resolved 5 s
# after the last one (emitter.overheat).
curl -X POST "$E1/condition/repeat?type=emitter.overheat&key=left&every=500ms"
curl -X POST "$E1/condition/stop?type=emitter.overheat&key=left"

# Said once, released by name (emitter.disconnected). The string "false" does
# not release it; the bool does.
curl -X POST "$E1/condition/once?type=emitter.disconnected&key=uplink"
curl -X POST "$E1/condition/release?type=emitter.disconnected&key=uplink&as=string"
curl -X POST "$E1/condition/release?type=emitter.disconnected&key=uplink"

# A plain ERROR from the reporter, which has no liveness: forgotten after a
# quiet minute (reporter.error).
curl -X POST localhost:18083/error/once

# Only a standard attribute (emitter.timeout).
curl -X POST "$E1/error/repeat?every=1s&error_type=timeout&server_address=db:5432"
curl -X POST "$E1/error/stop"

# Silent: no logs, the liveness counter goes on -- the evidence a silence
# resolves on.
curl -X POST "$E1/silence"
curl -X POST "$E1/silence/stop"

# Hung: the counter stops growing (emitter.stalled); crashed twice: a crash
# loop (emitter.crash_loop).
curl -X POST "$E1/hang"
curl -X POST "$E1/hang/stop"
curl -X POST "$E1/exit?code=1"; sleep 5; curl -X POST "$E1/exit?code=1"

# Levels, rates and states (emitter.battery.low, emitter.messages.slow,
# emitter.hardware).
E2=localhost:18082
curl -X POST "$E2/gauge?name=battery.charge&value=15&attr.battery.id=main"
curl -X POST "$E2/messages?stream=main&rate=10"
curl -X POST "$E2/messages?stream=main&rate=5"
curl -X POST "$E2/hw?id=disk0&state=failed"

curl $E1/    # what emitter-1 is doing
```

## Fault injection

What Ward is expected to make of each is in the design (§7.2 the node gate,
§7.6 restarts, §12.3).

```sh
cd deploy/compose
LAN=ward-local_lan

# A service is gone: no data points from its liveness counter (emitter.down).
docker compose stop emitter-1          # start it again with: docker compose start emitter-1
docker compose pause emitter-1         # gone without exiting; unpause to resume

# A node is gone: its Collector stops. The canary is refused, and the node is
# unobservable (node.unobservable); verdicts that rest on silence there wait.
docker compose stop edge-1
docker compose start edge-1

# A node cut off the network: its services still reach its Collector, whose
# queue holds what it cannot send; Ward's canary cannot reach it.
docker network disconnect $LAN $(docker compose ps -q edge-1)
docker network connect --alias edge-1 $LAN $(docker compose ps -q edge-1)

# A Collector restart: its queue is in memory, and lost.
docker compose restart edge-2

# Ward frozen: it accepts nothing and answers nothing, and the Collectors
# queue and retry, up to 120 s (engine.recovery_window).
docker compose pause ward
docker compose unpause ward

# Ward stopped, cleanly and not.
docker compose stop ward && docker compose start ward
docker compose kill -s KILL ward && docker compose start ward   # a power cut, near enough
```

`docker network connect` needs `--alias edge-1` again: the alias the Collector
had on the LAN, which Ward's `nodes.edge-1.collector` names, goes with the
disconnect.

None of this touches the services' own export: a Collector's queue answers
them at once whatever Ward does. A Collector that also runs an exporter
without a queue is the exception -- see the caveat in
`collector/collector.yaml`.

## The scenarios

`compose/e2e_test.go` drives the running environment through the emitters and
docker compose, and checks what Ward says over its API: silence, an explicit
release and a string that is not one, levels with a latch and a reset, a
discrete state, a process down and back, a crash loop, a node that cannot be
observed, and the change feed. It leaves the environment running, and takes
about four minutes.

```sh
go test -tags e2e -count=1 -v -run TestEndToEnd ./deploy/compose/
go test -tags e2e -count=1 -v -run TestFaults ./deploy/compose/    # Ward killed and frozen, a node cut off, a reboot
go test -tags e2e -count=1 -v -run TestMeasure ./deploy/compose/   # signal to verdict, in the API
```

`TestFaults` kills Ward with an Issue open (it comes back with it, and resolves
it once its nodes are observable again), freezes it inside the recovery window
(nothing is lost, and the services' own exports never fail), cuts a node off
the network (nothing on it resolves until it is back), and fakes a reboot with
a `system.uptime` of one second (what was open there is `interrupted.reboot`).
Run it after `TestEndToEnd`: the reboot it fakes stays in Ward's memory of the
node.

## Ward inside a Collector

`../collector/` builds a Collector with Ward in it: `wardexporter` is Ward as an
exporter, and `builder-config.yaml` the ocb manifest. Over the base file, it
takes the place of `ward serve` -- the nodes' Collectors send to it, the API is
on 14319, and the same scenarios run:

```sh
cd deploy/compose
docker compose -f docker-compose.yaml -f docker-compose.collector.yaml up -d --build
```

`compose/collector-ward.yaml` is its configuration: the Collector's OTLP
receiver, and the `ward` exporter whose `ward` block is `ward.yaml` without the
receivers (a test holds the two together).
