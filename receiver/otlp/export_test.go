package otlp

import "time"

// SetStopTimeout changes how long Run takes, at most, to stop once its context
// is done.
func SetStopTimeout(r *Receiver, d time.Duration) { r.stopTimeout = d }
