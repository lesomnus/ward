// Package robot is a sample: a robot's diagnostics in Ward. README.md says
// which program sends which signal, and rules.yaml is the rules. These tests
// show the conclusions: a model of the robot's programs sends the signals
// every 2 s through Ward's own normalizer and evaluator, a tick a second, and
// each test checks the alarms that open and close -- and that nothing else
// does.
//
//	go test -v ./example/robot/
package robot_test

import (
	"context"
	"fmt"
	"maps"
	"os"
	"slices"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
	"github.com/lesomnus/ward/engine"
)

const node = "robot-1"

var t0 = time.Date(2026, 11, 9, 9, 0, 0, 0, time.UTC)

// gate is the node gate as a scenario has it: observable unless down. How
// Ward finds out -- canaries through the robot's Collector -- is tested in the
// engine.
type gate struct {
	down  bool
	since time.Time
}

func (g *gate) Observable(string, engine.Path) (bool, string, time.Time) {
	if g.down {
		return false, engine.GateUnreachable, g.since
	}
	return true, "", g.since
}

// program is one process on the robot: its service.name, when it started,
// and its main loop's count.
type program struct {
	name     string
	started  time.Time
	instance string
	alive    bool // runs and exports
	looping  bool // its main loop turns
	loop     int64
}

func newProgram(name string, started time.Time, run int) *program {
	return &program{name: name, started: started, instance: fmt.Sprintf("%s-%d", name, run), alive: true, looping: true}
}

func (p *program) resource(res pcommon.Map) {
	res.PutStr("service.name", p.name)
	res.PutStr("service.instance.id", p.instance)
	res.PutStr("host.name", node) // the robot's Collector sets it
}

// robot is what its programs report, as README.md has them send it.
type robot struct {
	progs map[string]*program
	runs  int

	// robot-controller
	estop     map[string]int64
	board     map[string]int64
	locked    int64
	motor     map[string]string
	temp      map[string]float64
	stale     map[string]int64
	rtHz      float64
	rtCount   float64
	periodMax float64
	drops     map[string]int64

	// power-manager
	soc       float64
	socKnown  bool
	connected int64
	power     string

	// sensor-hub
	sensor map[string][2]string // id: kind, state

	// fault-reporter
	faults map[string]plog.SeverityNumber
}

const (
	controller = "robot-controller"
	powerMgr   = "power-manager"
	sensorHub  = "sensor-hub"
	reporter   = "fault-reporter"
)

func healthy(started time.Time) *robot {
	r := &robot{
		progs: map[string]*program{},
		estop: map[string]int64{"wired": 0, "software": 0, "wireless": 0},
		board: map[string]int64{"main": 0, "power": 0},
		motor: map[string]string{"m1": "ok", "m2": "ok", "m3": "ok", "m4": "ok"},
		temp:  map[string]float64{"m1": 40, "m2": 41, "m3": 39, "m4": 40},
		stale: map[string]int64{"j1": 0, "j2": 0, "j3": 0, "j4": 0, "j5": 0, "j6": 0},
		rtHz:  1000, periodMax: 1100,
		drops: map[string]int64{"cmd": 0, "state": 0},
		soc:   0.8, socKnown: true, connected: 1, power: "discharging",
		sensor: map[string][2]string{"cam_front": {"camera", "ok"}, "cam_rear": {"camera", "ok"}, "imu": {"imu", "ok"}},
		faults: map[string]plog.SeverityNumber{},
	}
	for _, name := range []string{controller, powerMgr, sensorHub, reporter} {
		r.progs[name] = newProgram(name, started, 1)
	}
	r.runs = 1
	return r
}

// restart is a program crashing and coming back: a new process, its counters
// from zero.
func (r *robot) restart(name string, at time.Time) {
	r.runs++
	r.progs[name] = newProgram(name, at, r.runs)
	if name == controller {
		r.rtCount = 0
		for q := range r.drops {
			r.drops[q] = 0
		}
	}
}

var (
	motorStates  = []string{"ok", "off", "disconnected", "error"}
	sensorStates = []string{"ok", "missing", "error"}
	powerStates  = []string{"idle", "discharging", "charging", "shutdown"}
)

// export is one export of every program alive at t.
func (r *robot) export(md pmetric.Metrics, t time.Time) {
	at := pcommon.NewTimestampFromTime(t)
	for _, name := range []string{controller, powerMgr, sensorHub, reporter} {
		p := r.progs[name]
		if !p.alive {
			continue
		}
		if p.looping {
			p.loop += 20 // a 10 Hz loop
		}
		if name == controller {
			r.rtCount += r.rtHz * 2
		}
		rm := md.ResourceMetrics().AppendEmpty()
		p.resource(rm.Resource().Attributes())
		ms := rm.ScopeMetrics().AppendEmpty().Metrics()
		start := pcommon.NewTimestampFromTime(p.started)

		counter := func(name string) pmetric.NumberDataPointSlice {
			m := ms.AppendEmpty()
			m.SetName(name)
			s := m.SetEmptySum()
			s.SetIsMonotonic(true)
			s.SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
			return s.DataPoints()
		}
		gauge := func(name string) pmetric.NumberDataPointSlice {
			m := ms.AppendEmpty()
			m.SetName(name)
			return m.SetEmptyGauge().DataPoints()
		}
		point := func(dps pmetric.NumberDataPointSlice, v float64, cumulative bool, attrs ...string) {
			dp := dps.AppendEmpty()
			dp.SetTimestamp(at)
			if cumulative {
				dp.SetStartTimestamp(start)
			}
			if v == float64(int64(v)) {
				dp.SetIntValue(int64(v))
			} else {
				dp.SetDoubleValue(v)
			}
			for i := 0; i+1 < len(attrs); i += 2 {
				dp.Attributes().PutStr(attrs[i], attrs[i+1])
			}
		}
		one := func(b bool) float64 {
			if b {
				return 1
			}
			return 0
		}

		point(counter("robot.loop"), float64(p.loop), true)

		switch name {
		case controller:
			dps := gauge("robot.estop.active")
			for _, k := range slices.Sorted(maps.Keys(r.estop)) {
				point(dps, float64(r.estop[k]), false, "estop.source", k)
			}
			dps = gauge("robot.board.fault")
			for _, k := range slices.Sorted(maps.Keys(r.board)) {
				point(dps, float64(r.board[k]), false, "board.id", k)
			}
			point(gauge("robot.joint_lock.locked"), float64(r.locked), false)
			dps = gauge("robot.motor.state")
			temps := gauge("robot.motor.temperature")
			for _, k := range slices.Sorted(maps.Keys(r.motor)) {
				for _, s := range motorStates {
					point(dps, one(r.motor[k] == s), false, "motor.id", k, "motor.state", s)
				}
				point(temps, r.temp[k], false, "motor.id", k)
			}
			dps = gauge("robot.joint.stale")
			for _, k := range slices.Sorted(maps.Keys(r.stale)) {
				point(dps, float64(r.stale[k]), false, "joint.id", k)
			}
			point(counter("robot.rt_cycle.count"), float64(int64(r.rtCount)), true)
			point(gauge("robot.rt_cycle.period_max"), r.periodMax, false)
			dps = counter("robot.sdk.push_drops")
			for _, k := range slices.Sorted(maps.Keys(r.drops)) {
				point(dps, float64(r.drops[k]), true, "queue.name", k)
			}
		case powerMgr:
			if r.socKnown {
				point(gauge("robot.battery.soc"), r.soc, false)
			}
			point(gauge("robot.battery.connected"), float64(r.connected), false)
			dps := gauge("robot.battery.power_state")
			for _, s := range powerStates {
				point(dps, one(r.power == s), false, "battery.power_state", s)
			}
		case sensorHub:
			dps := gauge("robot.sensor.state")
			for _, k := range slices.Sorted(maps.Keys(r.sensor)) {
				for _, s := range sensorStates {
					point(dps, one(r.sensor[k][1] == s), false, "sensor.id", k, "sensor.kind", r.sensor[k][0], "sensor.state", s)
				}
			}
		}
	}
}

// world runs the robot and Ward a tick a second.
type world struct {
	t    *testing.T
	r    *robot
	boot time.Time
	gate *gate

	norm *engine.Normalizer
	ev   *engine.Evaluator

	now  time.Time
	next time.Time // the next export
	logs plog.Logs
	mark time.Time
	seen []seen
}

type seen struct {
	tick time.Time
	tr   engine.Transition
}

func newWorld(t *testing.T) *world {
	t.Helper()
	b, err := os.ReadFile("rules.yaml")
	if err != nil {
		t.Fatal(err)
	}
	f, err := def.Parse(b)
	if err != nil {
		t.Fatal(err)
	}
	set, warns, err := def.Merge([]string{node}, def.Definitions{Provider: "robot", Revision: "sample", File: f})
	if err != nil || len(warns) > 0 {
		t.Fatalf("rules: %v %v", err, warns)
	}
	d := engine.Compile(set)

	w := &world{t: t, boot: t0.Add(-2 * time.Hour), now: t0, next: t0, logs: plog.NewLogs(), gate: &gate{since: t0.Add(-time.Hour)}}
	w.r = healthy(t0.Add(-time.Hour))
	g := engine.NewGate(conf.Gate{Hold: 15 * time.Second, UnobservableToStale: time.Minute,
		Canary: conf.Canary{Interval: time.Second, Timeout: 2 * time.Second, MaxRTT: 200 * time.Millisecond, CheckWindow: 30 * time.Second, MaxMissing: 1}},
		[]string{node})
	w.norm = engine.NewNormalizer(func() time.Time { return w.now }, map[string]map[string]string{node: {"host.name": node}}, "sample", g, 2*time.Minute)
	w.norm.Use(d)
	w.ev = engine.NewEvaluator(engine.Config{
		LagLog: 2 * time.Second, LagMetric: 3 * time.Second, UnobservableToStale: time.Minute,
		KeysPerRule: 50, NewIssuesPerMinute: 10, MaxShelve: 8 * time.Hour, Nodes: []string{node},
	}, w.gate)
	w.ev.Apply(d, t0)

	// A minute of the healthy robot first: nothing may open.
	w.run(60 * time.Second)
	if len(w.seen) > 0 {
		t.Fatalf("the healthy robot raised:\n%s", w.timeline())
	}
	return w
}

// fault has the fault-reporter raise or clear a fault code.
func (w *world) fault(code string, sev plog.SeverityNumber, raised bool) {
	if raised {
		w.r.faults[code] = sev
		w.record(code, sev, nil)
		return
	}
	delete(w.r.faults, code)
	f := false
	w.record(code, plog.SeverityNumberInfo, &f)
}

func (w *world) record(code string, sev plog.SeverityNumber, active *bool) {
	rl := w.logs.ResourceLogs().AppendEmpty()
	w.r.progs[reporter].resource(rl.Resource().Attributes())
	lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
	lr.SetTimestamp(pcommon.NewTimestampFromTime(w.now))
	lr.SetSeverityNumber(sev)
	lr.Body().SetStr("fault " + code)
	lr.Attributes().PutStr("diag.condition.type", "robot.fault")
	lr.Attributes().PutStr("diag.condition.key", code)
	if active != nil {
		lr.Attributes().PutBool("diag.condition.active", *active)
	}
}

func (w *world) run(d time.Duration) {
	until := w.now.Add(d)
	ctx := context.Background()
	for w.now.Before(until) {
		w.now = w.now.Add(time.Second)

		// Every 2 s the programs export, and the robot's Collector the uptime.
		for !w.next.After(w.now.Add(-500 * time.Millisecond)) {
			at := w.next
			md := pmetric.NewMetrics()
			w.r.export(md, at)
			rm := md.ResourceMetrics().AppendEmpty()
			rm.Resource().Attributes().PutStr("host.name", node)
			up := rm.ScopeMetrics().AppendEmpty().Metrics().AppendEmpty()
			up.SetName("system.uptime")
			dp := up.SetEmptyGauge().DataPoints().AppendEmpty()
			dp.SetTimestamp(pcommon.NewTimestampFromTime(at))
			dp.SetDoubleValue(at.Sub(w.boot).Seconds())
			w.norm.ConsumeMetrics(ctx, md)
			w.next = w.next.Add(2 * time.Second)
		}

		// While a fault lasts, the fault-reporter logs it again every 5 s.
		if w.r.progs[reporter].alive && w.now.Unix()%5 == 0 {
			for _, code := range slices.Sorted(maps.Keys(w.r.faults)) {
				w.record(code, w.r.faults[code], nil)
			}
		}
		if w.logs.LogRecordCount() > 0 {
			w.norm.ConsumeLogs(ctx, w.logs)
			w.logs = plog.NewLogs()
		}

		w.ev.Step(w.now, w.norm.Drain())
		ts, _ := w.ev.Take()
		for _, tr := range ts {
			w.seen = append(w.seen, seen{w.now, tr})
		}
	}
}

// start marks a scenario's beginning: what follows is timed from here.
func (w *world) start() {
	w.mark = w.now
	w.seen = nil
}

func line(tr engine.Transition) string {
	i := tr.Issue
	s := fmt.Sprintf("%s %s/%s %s", tr.Event, i.Rule, i.Key, i.Severity)
	if i.Level != "" {
		s += " level=" + i.Level
	}
	if tr.Event == engine.EventClose {
		s += " " + i.CloseReason
	}
	if tr.Event == engine.EventUpdate && i.State != engine.StateActive {
		s += " state=" + i.State
	}
	return s
}

func (w *world) timeline() string {
	var b strings.Builder
	for _, x := range w.seen {
		fmt.Fprintf(&b, "  +%3.0fs  %-72s (in the API +%.0fs)\n", x.tr.Date.Sub(w.mark).Seconds(), line(x.tr), x.tick.Sub(w.mark).Seconds())
	}
	return b.String()
}

// expect checks what opened, changed and closed since start, in order.
func (w *world) expect(want ...string) {
	w.t.Helper()
	var got []string
	for _, x := range w.seen {
		got = append(got, line(x.tr))
	}
	w.t.Logf("since the scenario began:\n%s", w.timeline())
	if !slices.Equal(got, want) {
		w.t.Errorf("got:\n  %s\nwant:\n  %s", strings.Join(got, "\n  "), strings.Join(want, "\n  "))
	}
}

// -- the programs and the robot ------------------------------------------------

func TestHealthyRobotRaisesNothing(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.run(5 * time.Minute)
	w.expect()
}

// The controller stops sending; it comes back as a new process. Its other
// conditions are unknown meanwhile, not alarms.
func TestControllerDown(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.progs[controller].alive = false
	w.run(20 * time.Second)
	w.r.restart(controller, w.now)
	w.run(40 * time.Second)
	w.expect(
		"open controller.down/ error level=down",
		"close controller.down/ error level=down resolved.condition",
	)
}

// Still exported, the controller's loop no longer turns.
func TestControllerStalled(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.progs[controller].looping = false
	w.run(20 * time.Second)
	w.r.progs[controller].looping = true
	w.run(30 * time.Second)
	w.expect(
		"open controller.stalled/ error level=stalled",
		"close controller.stalled/ error level=stalled resolved.condition",
	)
}

// Only the power-manager dies: one alarm, for it. The battery is unknown
// meanwhile -- not low, not disconnected.
func TestPowerManagerDown(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.progs[powerMgr].alive = false
	w.run(20 * time.Second)
	w.r.restart(powerMgr, w.now)
	w.run(40 * time.Second)
	w.expect(
		"open power-manager.down/ error level=down",
		"close power-manager.down/ error level=down resolved.condition",
	)
}

// The robot itself drops off -- powered off, or its network gone -- and nothing
// arrives from it. That is not its programs going down: Ward holds those
// verdicts, and says the robot cannot be observed.
func TestRobotUnobservable(t *testing.T) {
	w := newWorld(t)
	w.start()
	for _, p := range w.r.progs {
		p.alive = false
	}
	w.gate.down, w.gate.since = true, w.now
	w.run(30 * time.Second)
	w.gate.down, w.gate.since = false, w.now
	w.r = healthy(w.now)
	w.run(40 * time.Second)
	w.expect(
		"open robot.unobservable/ error level=log: unreachable",
		"close robot.unobservable/ error level=log: unreachable resolved.condition",
	)
}

// -- robot-controller -----------------------------------------------------------

// The software e-stop for 10 s; the other two stay quiet.
func TestEstop(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.estop["software"] = 1
	w.run(10 * time.Second)
	w.r.estop["software"] = 0
	w.run(10 * time.Second)
	w.expect(
		"open robot.estop/software error level=active",
		"close robot.estop/software error level=active resolved.condition",
	)
}

func TestBoardFaultAndJointLock(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.board["power"] = 1
	w.run(10 * time.Second)
	w.r.board["power"] = 0
	w.r.locked = 1
	w.run(10 * time.Second)
	w.r.locked = 0
	w.run(10 * time.Second)
	w.expect(
		"open robot.board_fault/power error level=fault",
		"close robot.board_fault/power error level=fault resolved.condition",
		"open robot.joint_locked/ error level=locked",
		"close robot.joint_locked/ error level=locked resolved.condition",
	)
}

// m2 disconnects, then reports an error, then recovers.
func TestMotorFault(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.motor["m2"] = "disconnected"
	w.run(10 * time.Second)
	w.r.motor["m2"] = "error"
	w.run(10 * time.Second)
	w.r.motor["m2"] = "ok"
	w.run(10 * time.Second)
	w.expect(
		"open robot.motor_fault/m2 error level=disconnected",
		"update robot.motor_fault/m2 error level=error",
		"close robot.motor_fault/m2 error level=error resolved.condition",
	)
}

// 6 s off is nothing; 20 s off is a state change worth an info.
func TestMotorOff(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.motor["m3"] = "off"
	w.run(6 * time.Second)
	w.r.motor["m3"] = "ok"
	w.run(10 * time.Second)
	w.r.motor["m3"] = "off"
	w.run(20 * time.Second)
	w.r.motor["m3"] = "ok"
	w.run(10 * time.Second)
	w.expect(
		"open robot.motor_off/m3 info level=off",
		"close robot.motor_off/m3 info level=off resolved.condition",
	)
}

// Warm for 10 s, then hot for 10 s, then cool.
func TestMotorTemperature(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.temp["m1"] = 70
	w.run(20 * time.Second)
	w.r.temp["m1"] = 85
	w.run(20 * time.Second)
	w.r.temp["m1"] = 79 // still hot: within the deadband of 80
	w.run(6 * time.Second)
	w.r.temp["m1"] = 50
	w.run(30 * time.Second)
	w.expect(
		"open robot.motor_temperature/m1 warning level=warm",
		"update robot.motor_temperature/m1 error level=hot",
		"close robot.motor_temperature/m1 error level=hot resolved.condition",
	)
}

// j3 gets no updates for 6 s.
func TestJointStale(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.stale["j3"] = 1
	w.run(6 * time.Second)
	w.r.stale["j3"] = 0
	w.run(10 * time.Second)
	w.expect(
		"open robot.joint_stale/j3 error level=stale",
		"close robot.joint_stale/j3 error level=stale resolved.condition",
	)
}

// 850 Hz for 25 s; then one export with a 2 ms period.
func TestRTCycle(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.rtHz = 850
	w.run(25 * time.Second)
	w.r.rtHz = 1000
	w.run(30 * time.Second)
	w.r.periodMax = 2000
	w.run(2 * time.Second)
	w.r.periodMax = 1100
	w.run(30 * time.Second)
	w.expect(
		"open robot.rt_cycle_slow/ warning level=slow",
		"close robot.rt_cycle_slow/ warning level=slow resolved.condition",
		"open robot.rt_cycle_late/ warning level=late",
		"close robot.rt_cycle_late/ warning level=late resolved.condition",
	)
}

// The cmd queue drops 5 in one export interval, then none.
func TestSDKDrops(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.drops["cmd"] += 5
	w.run(40 * time.Second)
	w.expect(
		"open robot.sdk_drops/cmd warning level=dropping",
		"close robot.sdk_drops/cmd warning level=dropping resolved.condition",
	)
}

// -- power-manager --------------------------------------------------------------

// Low, then critical, then charged again.
func TestBatteryLevels(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.soc = 0.15
	w.run(10 * time.Second)
	w.r.soc = 0.08
	w.run(10 * time.Second)
	w.r.soc = 0.9
	w.r.power = "charging"
	w.run(40 * time.Second)
	w.expect(
		"open robot.battery_low/ warning level=low",
		"update robot.battery_low/ error level=critical",
		"close robot.battery_low/ error level=critical resolved.condition",
	)
}

// Unplugged -- the charge is unknown, not low -- and a shutdown.
func TestBatteryDisconnectedAndShutdown(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.connected, w.r.socKnown = 0, false
	w.run(10 * time.Second)
	w.r.connected, w.r.socKnown = 1, true
	w.run(10 * time.Second)
	w.r.power = "shutdown"
	w.run(10 * time.Second)
	w.r.power = "idle"
	w.run(10 * time.Second)
	w.expect(
		"open robot.battery_disconnected/ error level=disconnected",
		"close robot.battery_disconnected/ error level=disconnected resolved.condition",
		"open robot.battery_power_state/ error level=shutdown",
		"close robot.battery_power_state/ error level=shutdown resolved.condition",
	)
}

// -- sensor-hub -----------------------------------------------------------------

// The rear camera goes missing, and comes back.
func TestSensorMissing(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.r.sensor["cam_rear"] = [2]string{"camera", "missing"}
	w.run(10 * time.Second)
	w.r.sensor["cam_rear"] = [2]string{"camera", "ok"}
	w.run(10 * time.Second)
	w.expect(
		"open robot.sensor_fault/cam_rear warning level=missing",
		"close robot.sensor_fault/cam_rear warning level=missing resolved.condition",
	)
}

// -- fault-reporter -------------------------------------------------------------

// An error code raised and cleared; a warning code raised and cleared; an
// error code whose clear was lost, closed by silence while the reporter is
// alive.
func TestFaultCodes(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.fault("E1203", plog.SeverityNumberError, true)
	w.run(20 * time.Second)
	w.fault("E1203", 0, false)
	w.fault("W0042", plog.SeverityNumberWarn, true)
	w.run(12 * time.Second)
	w.fault("W0042", 0, false)
	w.fault("E2001", plog.SeverityNumberError, true)
	w.run(12 * time.Second)
	delete(w.r.faults, "E2001") // cleared, and its clear never arrived
	w.run(30 * time.Second)
	w.expect(
		"open robot.fault_code/E1203 error",
		"close robot.fault_code/E1203 error resolved.explicit",
		"open robot.warning_code/W0042 warning",
		"close robot.warning_code/W0042 warning resolved.explicit",
		"open robot.fault_code/E2001 error",
		"close robot.fault_code/E2001 error resolved.silence",
	)
}

// The reporter restarts while a fault is raised: the alarm ends with the
// process that raised it -- interrupted, not resolved -- and the restarted
// reporter raising it again is a new alarm.
func TestFaultReporterRestart(t *testing.T) {
	w := newWorld(t)
	w.start()
	w.fault("E1203", plog.SeverityNumberError, true)
	w.run(12 * time.Second) // logged at +0, +5, +10
	faults := w.r.faults
	w.r.restart(reporter, w.now) // crashed, and restarted at +12
	w.r.faults = map[string]plog.SeverityNumber{}
	w.run(2 * time.Second)
	w.r.faults = faults
	w.fault("E1203", plog.SeverityNumberError, true) // still there: raised again at +14
	w.run(20 * time.Second)
	w.expect(
		"open robot.fault_code/E1203 error",
		"close robot.fault_code/E1203 error interrupted.source_restart",
		"open robot.fault_code/E1203 error",
	)
	// Back within a second, the reporter was never down long enough to be
	// missed: no fault-reporter.down.
}
