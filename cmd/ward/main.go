// Command ward is a payday app.
//
// What to read first is `cmd/serve.go`: it is the stack, written out, and it is
// the whole of what this app says to stand up a served, migrated, walled
// server. `cli` is the command line over it, and is a package of its own so
// that what only a process needs -- the database engine, the migration engine
// -- is not linked into a sandbox that needs neither.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"

	"github.com/lesomnus/ward/cli"
	"github.com/lesomnus/ward/cmd"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	var c cmd.Config
	if err := cli.Cmd(&c).Run(ctx, os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "ward:", err)
		os.Exit(1)
	}
}
