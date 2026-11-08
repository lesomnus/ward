package wardexporter

import (
	"errors"
	"fmt"
	"os"

	"github.com/lesomnus/xli/cfg"
	"gopkg.in/yaml.v3"

	"github.com/lesomnus/ward/cmd"
)

// Config is the exporter's block of a Collector's configuration.
//
// Nothing has a default: the default configuration is empty, and Validate
// refuses what was left out, as `ward serve` does.
type Config struct {
	// QueueSize is how many of the pipeline's hand-overs Ward holds before it
	// drops what comes next. What it drops shows as a gap in the canaries.
	QueueSize int `mapstructure:"queue_size"`

	// Ward is Ward's static configuration -- what a ward.yaml holds -- but its
	// receivers: the Collector's own deliver the signals.
	Ward map[string]any `mapstructure:"ward"`
}

// Validate checks the block as Ward checks its configuration.
func (c *Config) Validate() error {
	var errs []error
	if c.QueueSize <= 0 {
		errs = append(errs, errors.New("queue_size: missing or not positive"))
	}
	if _, err := c.load(); err != nil {
		errs = append(errs, err)
	}
	return errors.Join(errs...)
}

// load reads the ward block with the loader `ward serve` reads its file with.
func (c *Config) load() (cmd.Config, error) {
	if len(c.Ward) == 0 {
		return cmd.Config{}, errors.New("ward: missing")
	}
	b, err := yaml.Marshal(c.Ward)
	if err != nil {
		return cmd.Config{}, fmt.Errorf("ward: %w", err)
	}
	f, err := os.CreateTemp("", "ward-*.yaml")
	if err != nil {
		return cmd.Config{}, err
	}
	defer os.Remove(f.Name())
	if _, err := f.Write(b); err != nil {
		f.Close()
		return cmd.Config{}, err
	}
	if err := f.Close(); err != nil {
		return cmd.Config{}, err
	}
	s, err := cfg.NewFile[cmd.Config](f.Name()).Read()
	if err != nil {
		return cmd.Config{}, fmt.Errorf("ward: %w", err)
	}
	if err := s.Config.Ward().ValidateEmbedded(); err != nil {
		return cmd.Config{}, fmt.Errorf("ward:\n%w", err)
	}
	return *s.Config, nil
}
