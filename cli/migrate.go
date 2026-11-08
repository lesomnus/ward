package cli

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"

	"github.com/lesomnus/otx/log"
	"github.com/lesomnus/xli"
	"github.com/lesomnus/xli/arg"
	"github.com/lesomnus/xli/flg"
	"github.com/protobuf-orm/ent/dialect"
	entschema "github.com/protobuf-orm/ent/dialect/sql/schema"

	"github.com/lesomnus/payday/config"

	"github.com/lesomnus/ward/cmd"
	entmigrate "github.com/lesomnus/ward/internal/ent/migrate"
	"github.com/lesomnus/ward/migrations"
)

// NewCmdMigrate is `ward migrate`: the schema's history.
func NewCmdMigrate(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "migrate",
		Brief: "the schema's migrations",

		Commands: []*xli.Command{newCmdMigratePlan()},

		Handler: xli.RequireSubcommand(),
	}
}

func newCmdMigratePlan() *xli.Command {
	return &xli.Command{
		Name:  "plan",
		Brief: "write the SQL that brings the migrations up to the schema",
		Synop: "ward migrate plan [--dir DIR] NAME",

		Flags: flg.Flags{
			&flg.String{Name: "dir", Brief: "the migration directory; migrations by default"},
		},
		Args: arg.Args{
			&arg.String{Name: "NAME", Brief: "what the change is, e.g. add_issue_level"},
		},

		Handler: xli.OnRun(func(ctx context.Context, self *xli.Command, next xli.Next) error {
			name, _ := arg.Get[string](self, "NAME")
			dir, _ := flg.Find[string](self, "dir")
			if dir == "" {
				dir = "migrations"
			}

			files, err := Plan(ctx, dir, name)
			if err != nil {
				return err
			}
			if len(files) == 0 {
				fmt.Println("nothing to plan: the migrations already bring a database to the schema")
			}
			for _, f := range files {
				fmt.Println(f)
			}

			return next(ctx)
		}),
	}
}

// Plan writes into dir the SQL that brings the migrations already there up to
// the ent schema, and answers with the files it wrote. The files are replayed
// onto a scratch SQLite database, which is removed after.
func Plan(ctx context.Context, dir string, name string) ([]string, error) {
	d, err := entschema.OpenDir(dir)
	if err != nil {
		return nil, err
	}

	tmp, err := os.MkdirTemp("", "ward-migrate-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(tmp)

	dev, _, err := config.DbConfig{
		Driver: "sqlite3",
		Dsn:    "file:" + filepath.Join(tmp, "dev.db") + "?_pragma=foreign_keys(1)",
	}.Open(ctx)
	if err != nil {
		return nil, err
	}
	defer dev.Close()

	m := entschema.Migrations{Dir: d, Dialect: dialect.SQLite, Tables: entmigrate.Tables}
	fs, err := m.Plan(ctx, dev, name)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(fs))
	for i, f := range fs {
		names[i] = filepath.Join(dir, f.Name())
	}
	return names, nil
}

// Migrate brings the database s runs on to the schema this binary was built
// with: the embedded migrations, each run once and in order. A database with
// files still to run is copied beside itself first. A database that still
// disagrees with the schema afterwards is refused, so the server does not
// start on it.
func Migrate(ctx context.Context, s *cmd.Server) error {
	m, err := migrations.Embedded(s.Dialect)
	if err != nil {
		return err
	}

	pending, err := m.Pending(ctx, s.Db, s.Dialect)
	if err != nil {
		return err
	}
	if len(pending) > 0 {
		to, err := backup(ctx, s.Db, s.Dialect, pending[0].Version())
		if err != nil {
			return fmt.Errorf("back up the database before migrating it: %w", err)
		}
		if to != "" {
			log.From(ctx).InfoContext(ctx, "db: backed up before migrating", slog.String("to", to))
		}

		applied, err := m.Apply(ctx, s.Db, s.Dialect)
		if err != nil {
			return err
		}
		for _, f := range applied {
			log.From(ctx).InfoContext(ctx, "db: migrated", slog.String("file", f.Name()))
		}
	}

	if err := m.Current(ctx, s.Db, s.Dialect); err != nil {
		return err
	}
	return entschema.Check(ctx, s.Db, s.Dialect, entmigrate.Tables)
}

// backup copies the database file beside itself, named after the first
// migration about to run, and answers with where. A database in memory, or one
// with no tables yet, has nothing worth keeping.
func backup(ctx context.Context, db *sql.DB, d string, version string) (string, error) {
	if d != dialect.SQLite {
		return "", nil
	}

	var (
		seq        int
		name, file string
	)
	if err := db.QueryRowContext(ctx, "PRAGMA database_list").Scan(&seq, &name, &file); err != nil {
		return "", err
	}
	if file == "" {
		return "", nil
	}

	var tables int
	if err := db.QueryRowContext(ctx, "SELECT count(*) FROM sqlite_master WHERE type = 'table'").Scan(&tables); err != nil {
		return "", err
	}
	if tables == 0 {
		return "", nil
	}

	to := file + "." + version + ".bak"
	if _, err := db.ExecContext(ctx, "VACUUM INTO ?", to); err != nil {
		return "", err
	}
	return to, nil
}
