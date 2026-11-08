// Package provider is where Ward's sources and rules come from while it runs.
//
// A Provider supplies one set of definitions. The file provider, the first
// kind, reads one file: it sends what the file says once at start, then again
// each time that changes. The Registry runs every provider the static
// configuration names and puts their definitions in effect together: a new
// supply is checked with what every other provider has in effect (def.Merge),
// and either takes effect as a whole -- the merged Set is handed to the
// evaluation loop -- or is refused, in which case that provider's previous
// revision stays in effect and why is in its Status, which ProviderService
// reports. One broken file therefore stops neither the other providers' rules
// nor its own previous ones.
//
// The list of providers is static configuration: adding one is a restart.
// What a provider supplies is not.
package provider

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"

	"github.com/lesomnus/ward/conf"
	"github.com/lesomnus/ward/def"
)

// Update is one provider's supply: a new revision of its definitions, or why
// they could not be read.
type Update struct {
	// The provider's name in the static configuration.
	Provider string

	// Revision names the content supplied (see Revision). It is set with File,
	// and with Err when the content was read but does not parse; it is empty
	// when there was nothing to read.
	Revision string

	File *def.File

	// Err says why there is no File: the content could not be read, or does
	// not parse.
	Err error
}

// Provider supplies sources and rules.
type Provider interface {
	// Name is the provider's name in the static configuration.
	Name() string

	// Type is the kind of provider: file.
	Type() string

	// Location is where the definitions are read from: a file's path.
	Location() string

	// Run sends the current definitions once, then again each time they
	// change, until ctx is done. It does not send what it last sent again. It
	// returns nil once ctx is done; an error means it cannot supply anything
	// any more.
	Run(ctx context.Context, out chan<- Update) error
}

// New makes the provider c configures.
func New(c conf.Provider) (Provider, error) {
	switch c.Type {
	case "file":
		f, err := NewFile(c)
		if err != nil {
			return nil, err
		}
		return f, nil
	}
	return nil, fmt.Errorf("provider %s: type: %q is not a provider type; file is", c.Name, c.Type)
}

// Revision names content by what it is: the first 12 hex digits of its
// SHA-256, which is what `sha256sum FILE | cut -c1-12` prints for a file. The
// same content is the same revision wherever it was read from.
func Revision(content []byte) string {
	return revision(sha256.Sum256(content))
}

func revision(sum [sha256.Size]byte) string {
	return hex.EncodeToString(sum[:6])
}
