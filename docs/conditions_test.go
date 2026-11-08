package docs_test

import (
	"os"
	"regexp"
	"strings"
	"testing"

	"github.com/lesomnus/ward/def"
)

// The examples in conditions.md are what Ward accepts: its sources block,
// every block of rules and its components block make one provider file, which
// merges without a warning over the nodes its static configuration names.
func TestConditionsExamples(t *testing.T) {
	b, err := os.ReadFile("conditions.md")
	if err != nil {
		t.Fatal(err)
	}
	var (
		sources, components string
		rules               []string
	)
	for _, m := range regexp.MustCompile("(?s)```yaml\n(.*?)```").FindAllStringSubmatch(string(b), -1) {
		switch body := m[1]; {
		case strings.HasPrefix(body, "sources:"):
			sources = body
		case strings.HasPrefix(body, "components:"):
			components = body
		case strings.HasPrefix(body, "- id:"):
			rules = append(rules, body)
		}
	}
	if sources == "" || components == "" || len(rules) == 0 {
		t.Fatalf("found sources %v, components %v, %d blocks of rules", sources != "", components != "", len(rules))
	}

	var f strings.Builder
	f.WriteString("version: 1\n")
	f.WriteString(sources)
	f.WriteString("rules:\n")
	n := 0
	for _, r := range rules {
		for _, line := range strings.Split(strings.TrimRight(r, "\n"), "\n") {
			if strings.HasPrefix(line, "- id:") {
				n++
			}
			f.WriteString("  " + line + "\n")
		}
	}
	f.WriteString(components)

	file, err := def.Parse([]byte(f.String()))
	if err != nil {
		t.Fatalf("parse:\n%v\n\n%s", err, f.String())
	}
	set, warns, err := def.Merge([]string{"edge-1", "edge-2"}, def.Definitions{Provider: "doc", Revision: "r", File: file})
	if err != nil {
		t.Fatalf("merge:\n%v", err)
	}
	if len(warns) > 0 {
		t.Errorf("warnings: %v", warns)
	}
	if len(set.Rules) != n {
		t.Errorf("%d rules merged of the %d written", len(set.Rules), n)
	}
}
