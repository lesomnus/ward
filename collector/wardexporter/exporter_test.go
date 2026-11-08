package wardexporter

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/component/componenttest"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	app "github.com/lesomnus/ward"
)

func TestDefaultIsRefused(t *testing.T) {
	err := NewFactory().CreateDefaultConfig().(*Config).Validate()
	if err == nil {
		t.Fatal("the empty default configuration validated")
	}
	for _, want := range []string{"queue_size", "ward: missing"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("%q does not say %s", err, want)
		}
	}
}

const rules = `
version: 1
sources:
  app: {liveness: none}
rules:
  - id: app.error
    kind: log
    source: app
    severity: error
    match: {severity: {gte: error}}
    key: []
    activate: {count: 1}
    forget_after: 1h
    operator: {ack: none, latch: false}
`

func config(t *testing.T) (*Config, string) {
	dir := t.TempDir()
	path := filepath.Join(dir, "app.yaml")
	if err := os.WriteFile(path, []byte(rules), 0o644); err != nil {
		t.Fatal(err)
	}
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return &Config{
		QueueSize: 16,
		Ward: map[string]any{
			"server": map[string]any{"addr": addr},
			"db": map[string]any{
				"driver": "sqlite3",
				"dsn":    "file:" + filepath.Join(dir, "ward.db") + "?_pragma=journal_mode(WAL)&_pragma=foreign_keys(1)&_pragma=busy_timeout(5000)",
			},
			"watch":    map[string]any{"broker": "none"},
			"spin":     map[string]any{"lease": "local"},
			"operator": map[string]any{"max_shelve": "1h"},
			"engine": map[string]any{
				"tick": "100ms", "eval_lag": map[string]any{"log": "200ms", "metric": "300ms"},
				"recovery_window": "10s", "checkpoint": "1s",
				"gate": map[string]any{"hold": "1s", "unobservable_to_stale": "5s", "canary": map[string]any{
					"interval": "100ms", "timeout": "1s", "max_rtt": "500ms", "check_window": "1s", "max_missing": 1,
				}},
				"limits": map[string]any{"keys_per_rule": 10, "new_issues_per_rule_per_minute": 10},
			},
			"nodes":     map[string]any{"n1": map[string]any{"match": map[string]any{"host.name": "n1"}, "collector": "127.0.0.1:1"}},
			"providers": []any{map[string]any{"name": "app", "type": "file", "path": path, "poll_interval": "1s", "debounce": "50ms"}},
			"store":     map[string]any{"retention": "1h"},
			"exporters": []any{},
		},
	}, addr
}

func TestOneWardForLogsAndMetrics(t *testing.T) {
	ctx := context.Background()
	c, addr := config(t)
	if err := c.Validate(); err != nil {
		t.Fatalf("validate: %v", err)
	}
	set := exporter.Settings{ID: component.NewID(typ), TelemetrySettings: componenttest.NewNopTelemetrySettings()}
	f := NewFactory()
	logs, err := f.CreateLogs(ctx, set, c)
	if err != nil {
		t.Fatal(err)
	}
	metrics, err := f.CreateMetrics(ctx, set, c)
	if err != nil {
		t.Fatal(err)
	}
	host := componenttest.NewNopHost()
	if err := logs.Start(ctx, host); err != nil {
		t.Fatalf("start logs: %v", err)
	}
	if err := metrics.Start(ctx, host); err != nil {
		t.Fatalf("start metrics: one Ward should be started once: %v", err)
	}

	conn, err := grpc.NewClient(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	issues := app.NewIssueServiceClient(conn)

	rule := "app.error"
	deadline := time.Now().Add(10 * time.Second)
	for {
		ld := plog.NewLogs()
		rl := ld.ResourceLogs().AppendEmpty()
		rl.Resource().Attributes().PutStr("service.name", "app")
		rl.Resource().Attributes().PutStr("host.name", "n1")
		lr := rl.ScopeLogs().AppendEmpty().LogRecords().AppendEmpty()
		lr.SetTimestamp(pcommon.NewTimestampFromTime(time.Now()))
		lr.SetSeverityNumber(plog.SeverityNumberError)
		if err := logs.ConsumeLogs(ctx, ld); err != nil {
			t.Fatalf("an exporter error goes back to the apps: %v", err)
		}
		res, err := issues.List(ctx, app.IssueListRequest_builder{Filters: []*app.IssueFilter{app.IssueFilter_builder{Rule: &rule}.Build()}}.Build())
		if err == nil && len(res.GetItems()) > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("no issue: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}

	sctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := logs.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}
	// Still serving: the metrics exporter is the same Ward.
	if _, err := issues.List(ctx, app.IssueListRequest_builder{}.Build()); err != nil {
		t.Fatalf("stopped with one of its two exporters: %v", err)
	}
	if err := metrics.Shutdown(sctx); err != nil {
		t.Fatal(err)
	}
}
