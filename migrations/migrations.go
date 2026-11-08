// Package migrations is Ward's schema history: SQL files planned from the ent
// schema by `ward migrate plan`, reviewed like any other code, and applied in
// order by `ward serve`. They are embedded, so a binary migrates with exactly
// the files it was built with.
package migrations

import (
	"embed"
	"io/fs"
	"strings"

	"ariga.io/atlas/sql/migrate"
	entschema "github.com/protobuf-orm/ent/dialect/sql/schema"

	entmigrate "github.com/lesomnus/ward/internal/ent/migrate"
)

// Everything in this directory, this file included; Embedded keeps only the
// migration files and their sum. A pattern naming *.sql would not compile
// before the first file was planned.
//
//go:embed *
var files embed.FS

// Embedded is the migrations this binary was built with, written for dialect.
func Embedded(dialect string) (entschema.Migrations, error) {
	dir := migrate.OpenMemDir("ward-migrations")

	entries, err := fs.ReadDir(files, ".")
	if err != nil {
		return entschema.Migrations{}, err
	}
	for _, e := range entries {
		name := e.Name()
		if !strings.HasSuffix(name, ".sql") && name != migrate.HashFileName {
			continue
		}
		b, err := files.ReadFile(name)
		if err != nil {
			return entschema.Migrations{}, err
		}
		if err := dir.WriteFile(name, b); err != nil {
			return entschema.Migrations{}, err
		}
	}

	return entschema.Migrations{Dir: dir, Dialect: dialect, Tables: entmigrate.Tables}, nil
}
