package hub

// Metrics records hub activity for observability. All methods must be safe for
// concurrent use and cheap (they are called on hot paths). The interface is
// defined here, at the point of use, so the hub has no dependency on any metrics
// library; an implementation lives in package metrics and is wired in by main.
//
// For a C# developer: this is the same seam as an injected IMeterFactory /
// metrics abstraction — the hub emits events, the adapter decides how to record
// them.
type Metrics interface {
	ConnOpened()
	ConnClosed()
	BoardOpened()
	BoardClosed()
	MessageReceived()        // one message received from a client
	MessagesDelivered(n int) // n messages fanned out to clients
	ClientKicked()           // a client was disconnected for falling behind
}

// nopMetrics is the default when no Metrics is configured: every method is a
// no-op, so instrumented code never has to nil-check.
type nopMetrics struct{}

func (nopMetrics) ConnOpened()           {}
func (nopMetrics) ConnClosed()           {}
func (nopMetrics) BoardOpened()          {}
func (nopMetrics) BoardClosed()          {}
func (nopMetrics) MessageReceived()      {}
func (nopMetrics) MessagesDelivered(int) {}
func (nopMetrics) ClientKicked()         {}
