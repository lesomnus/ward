package cli

// The witnesses this app's configuration can name, `audit.checkpoints.witnesses`:
// where the newest checkpoint of the trail's chain is heard outside the database
// and the archive (payday's `trail.Witness`). Blank-imported here, beside the
// driver and for its reason: a process hears checkpoints, and the page does not.
//
// `log:` tells the log and the metrics, which the monitoring outside alerts on;
// `store:` and a directory keeps what it hears on a disk of its own, best one
// that refuses deletion and is mounted with credentials the database's and the
// archive's are not. Another is another line here.
import (
	_ "github.com/lesomnus/payday/config/witnesslog"
	_ "github.com/lesomnus/payday/config/witnessstore"
)
