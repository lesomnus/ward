// Package compose_test checks the local environment's configuration the way
// Ward reads it: ward.yaml as `ward --config` loads it, every provider file as
// a FileProvider parses it, merged as Ward merges them. An example that has
// drifted from what Ward accepts fails here rather than in front of whoever
// brings the environment up next.
package compose_test

import (
	"maps"
	"net"
	"os"
	"path"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/lesomnus/xli/cfg"
	"gopkg.in/yaml.v3"

	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/def"
)

// mounted is where docker-compose.yaml mounts ./providers in Ward's container,
// and so where ward.yaml says the provider files are.
const mounted = "/etc/ward/providers"

func config(t *testing.T) *cmd.Config {
	t.Helper()
	s, err := cfg.NewFile[cmd.Config]("ward.yaml").Read()
	if err != nil {
		t.Fatalf("ward.yaml: %v", err)
	}
	return s.Config
}

func TestConfigIsComplete(t *testing.T) {
	if err := config(t).Ward().Validate(); err != nil {
		t.Errorf("ward.yaml:\n%v", err)
	}
}

// merge parses every provider ward.yaml names, from ./providers, and merges
// them over its nodes.
func merge(t *testing.T, c *cmd.Config) *def.Set {
	t.Helper()

	var (
		ds    []def.Definitions
		files = map[string]bool{}
	)
	for _, p := range c.Providers {
		dir, file := path.Split(p.Path)
		if path.Clean(dir) != mounted {
			t.Errorf("provider %s: %s is not in %s, where ./providers is mounted", p.Name, p.Path, mounted)
			continue
		}
		files[file] = true

		b, err := os.ReadFile(filepath.Join("providers", file))
		if err != nil {
			t.Errorf("provider %s: %v", p.Name, err)
			continue
		}
		f, err := def.Parse(b)
		if err != nil {
			t.Errorf("provider %s: providers/%s:\n%v", p.Name, file, err)
			continue
		}
		ds = append(ds, def.Definitions{Provider: p.Name, Revision: "test", File: f})
	}

	entries, err := os.ReadDir("providers")
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".yaml") && !files[e.Name()] {
			t.Errorf("providers/%s: no provider of ward.yaml reads it", e.Name())
		}
	}
	if t.Failed() {
		t.FailNow()
	}

	s, warns, err := def.Merge(slices.Sorted(maps.Keys(c.Nodes)), ds...)
	if err != nil {
		t.Fatalf("merge:\n%v", err)
	}
	for _, w := range warns {
		t.Errorf("merge: %s", w)
	}
	return s
}

func TestProvidersMerge(t *testing.T) {
	s := merge(t, config(t))
	if len(s.Rules) == 0 {
		t.Error("no rules")
	}
}

// compose is what this test reads of docker-compose.yaml.
type compose struct {
	Services map[string]struct {
		Environment map[string]string `yaml:"environment"`
	} `yaml:"services"`
}

// TestComposeAgrees checks ward.yaml and the providers against
// docker-compose.yaml: each node's canary goes to a Collector there, which
// names its node as the node's match says, and each service sending there is
// a source.
func TestComposeAgrees(t *testing.T) {
	c := config(t)
	s := merge(t, c)

	b, err := os.ReadFile("docker-compose.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var dc compose
	if err := yaml.Unmarshal(b, &dc); err != nil {
		t.Fatalf("docker-compose.yaml: %v", err)
	}

	for _, name := range slices.Sorted(maps.Keys(c.Nodes)) {
		n := c.Nodes[name]
		host, _, err := net.SplitHostPort(n.Collector)
		if err != nil {
			t.Errorf("nodes.%s.collector: %v", name, err)
			continue
		}
		svc, ok := dc.Services[host]
		if !ok {
			t.Errorf("nodes.%s.collector: %s is no service of docker-compose.yaml", name, host)
			continue
		}
		if got, want := svc.Environment["NODE_HOST_NAME"], n.Match["host.name"]; got != want {
			t.Errorf("nodes.%s: matches host.name %q, and Collector %s sets %q", name, want, host, got)
		}
	}

	for _, name := range slices.Sorted(maps.Keys(dc.Services)) {
		svc := dc.Services[name]
		if source, ok := svc.Environment["OTEL_SERVICE_NAME"]; ok {
			if _, ok := s.Sources[source]; !ok {
				t.Errorf("service %s sends as %q, which no provider defines as a source", name, source)
			}
		}
	}
}
