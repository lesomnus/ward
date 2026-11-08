package cli_test

import (
	"maps"
	"os"
	"slices"
	"testing"

	"github.com/lesomnus/xli/cfg"

	"github.com/lesomnus/ward/cmd"
	"github.com/lesomnus/ward/def"
)

// The repository's ward.yaml is what `go run ./cmd/ward serve` starts on: it
// has every value, and its providers merge.
func TestRepositoryConfig(t *testing.T) {
	s, err := cfg.NewFile[cmd.Config]("../ward.yaml").Read()
	if err != nil {
		t.Fatalf("ward.yaml: %v", err)
	}
	w := s.Config.Ward()
	if err := w.Validate(); err != nil {
		t.Fatalf("ward.yaml:\n%v", err)
	}
	var ds []def.Definitions
	for _, p := range w.Providers {
		b, err := os.ReadFile("../" + p.Path)
		if err != nil {
			t.Fatal(err)
		}
		f, err := def.Parse(b)
		if err != nil {
			t.Fatalf("%s: %v", p.Path, err)
		}
		ds = append(ds, def.Definitions{Provider: p.Name, File: f})
	}
	if _, warns, err := def.Merge(slices.Sorted(maps.Keys(w.Nodes)), ds...); err != nil || len(warns) > 0 {
		t.Fatalf("providers: %v %v", err, warns)
	}
}
