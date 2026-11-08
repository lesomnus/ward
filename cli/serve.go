package cli

import (
	"context"
	"log/slog"
	"net"
	"slices"

	"github.com/lesomnus/otx/log"
	"github.com/lesomnus/xli"
	"golang.org/x/sync/errgroup"

	"github.com/lesomnus/payday/spin"

	"github.com/lesomnus/ward/cmd"
)

// NewCmdServe is `ward serve`: bring the database up to date, then answer.
//
// The migrations always run first (Migrate): a server that answered on a
// database it disagrees with would fail per request, in whichever handler
// touches what moved.
func NewCmdServe(c *cmd.Config) *xli.Command {
	return &xli.Command{
		Name:  "serve",
		Brief: "bring the database up to date, then answer requests",

		Handler: xli.OnRun(func(ctx context.Context, _ *xli.Command, next xli.Next) error {
			ctx, done, err := Telemetry(ctx, c)
			if err != nil {
				return err
			}
			defer done()

			s, err := cmd.Build(ctx, *c)
			if err != nil {
				return err
			}
			defer s.Close()

			if err := Migrate(ctx, s); err != nil {
				return err
			}

			l, err := net.Listen("tcp", c.Server.ListenAddr())
			if err != nil {
				return err
			}
			log.From(ctx).InfoContext(ctx, "grpc", slog.String("addr", l.Addr().String()))

			lease, err := c.Spin.Build(ctx, c.Db)
			if err != nil {
				return err
			}

			g, ctx := errgroup.WithContext(ctx)
			g.Go(func() error { return spin.Run(ctx, slices.Values(s.Spin), spin.WithLease(lease)) })
			g.Go(func() error { return s.Serve(ctx, *c, l) })

			return g.Wait()
		}),
	}
}
