package cli

// The one driver this app runs on in a process. It is blank-imported by the app
// rather than by payday so that an app does not carry an engine it never opens.
//
// It is in this package and not in `cmd` because a blank import is a property of
// the package that writes it, and `cmd` is what an app's sandbox imports for
// `Build`. A driver named there is linked into the page as well -- and this one
// brings SQLite compiled to Wasm and run on wazero, which is wasm inside wasm.
// The page opens `config/dbsqlite3wasm` instead: SQLite in a Worker beside it.
//
// Another database is another line here -- `config/dbpgx` for PostgreSQL -- and
// still not in `cmd`, for the same reason.
import _ "github.com/lesomnus/payday/config/dbsqlite3"
