package cli

// The language a Watch filter is written in: CEL, which `ResourceService.Watch`
// evaluates for a subscription that sends one (payday's
// `docs/guide/resources.md`). Without this line the process serves Batch, and a
// Watch with no filter, and refuses one that has a filter.
//
// It is here and not in `cmd` for the reason the driver beside it is: `cmd` is
// what an app's sandbox imports for `Build`, and a blank import is a property
// of the package that writes it. CEL's parser, checker and interpreter, ANTLR
// under the parser, and `text/template` for a prompt nothing renders were 10 MB
// of payday's own sandbox module, for a service a page does not serve.
import _ "github.com/lesomnus/payday/resource/resourcecel"
