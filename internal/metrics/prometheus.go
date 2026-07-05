// Package metrics provides a Prometheus implementation of the hub's Metrics
// interface and the HTTP handler that exposes it. It is the adapter that turns
// the hub's observability events into a scrapeable /metrics endpoint.
package metrics

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Prometheus records hub activity into Prometheus metrics. It satisfies
// hub.Metrics structurally. Use its Handler for the /metrics endpoint.
type Prometheus struct {
	reg *prometheus.Registry

	activeConns  prometheus.Gauge
	activeBoards prometheus.Gauge
	received     prometheus.Counter
	delivered    prometheus.Counter
	kicked       prometheus.Counter
}

// NewPrometheus builds the metrics into a private registry (which also includes
// the standard Go runtime and process collectors).
func NewPrometheus() *Prometheus {
	p := &Prometheus{
		reg: prometheus.NewRegistry(),
		activeConns: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "whiteboard", Name: "active_connections",
			Help: "Number of currently connected WebSocket clients.",
		}),
		activeBoards: prometheus.NewGauge(prometheus.GaugeOpts{
			Namespace: "whiteboard", Name: "active_boards",
			Help: "Number of boards with at least one connected client on this instance.",
		}),
		received: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "whiteboard", Name: "messages_received_total",
			Help: "Total messages received from clients.",
		}),
		delivered: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "whiteboard", Name: "messages_delivered_total",
			Help: "Total messages fanned out to clients.",
		}),
		kicked: prometheus.NewCounter(prometheus.CounterOpts{
			Namespace: "whiteboard", Name: "clients_kicked_total",
			Help: "Total clients disconnected for failing to keep up (backpressure).",
		}),
	}
	p.reg.MustRegister(
		p.activeConns, p.activeBoards, p.received, p.delivered, p.kicked,
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	return p
}

func (p *Prometheus) ConnOpened()             { p.activeConns.Inc() }
func (p *Prometheus) ConnClosed()             { p.activeConns.Dec() }
func (p *Prometheus) BoardOpened()            { p.activeBoards.Inc() }
func (p *Prometheus) BoardClosed()            { p.activeBoards.Dec() }
func (p *Prometheus) MessageReceived()        { p.received.Inc() }
func (p *Prometheus) MessagesDelivered(n int) { p.delivered.Add(float64(n)) }
func (p *Prometheus) ClientKicked()           { p.kicked.Inc() }

// Handler serves the metrics in the Prometheus text exposition format.
func (p *Prometheus) Handler() http.Handler {
	return promhttp.HandlerFor(p.reg, promhttp.HandlerOpts{})
}
