package compose_test

import (
	"os"
	"reflect"
	"testing"

	"gopkg.in/yaml.v3"
)

// collector-ward.yaml runs the same Ward as ward.yaml, inside a Collector: its
// ward block is ward.yaml but the receivers.
func TestCollectorConfigAgrees(t *testing.T) {
	read := func(name string) map[string]any {
		b, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		var v map[string]any
		if err := yaml.Unmarshal(b, &v); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		return v
	}
	ward := read("ward.yaml")
	delete(ward, "receivers")
	in := read("collector-ward.yaml")["exporters"].(map[string]any)["ward"].(map[string]any)["ward"].(map[string]any)
	if !reflect.DeepEqual(ward, in) {
		t.Errorf("collector-ward.yaml's ward block is not ward.yaml without receivers:\n%v\n%v", ward, in)
	}
}
