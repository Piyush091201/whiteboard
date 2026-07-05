package metrics_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Piyush091201/whiteboard/internal/metrics"
)

// TestPrometheusExposition records some activity and asserts it appears in the
// scrape output in the Prometheus text format.
func TestPrometheusExposition(t *testing.T) {
	p := metrics.NewPrometheus()

	p.ConnOpened()
	p.ConnOpened()
	p.ConnClosed()
	p.BoardOpened()
	p.MessageReceived()
	p.MessagesDelivered(3)
	p.ClientKicked()

	req := httptest.NewRequest("GET", "/metrics", nil)
	rec := httptest.NewRecorder()
	p.Handler().ServeHTTP(rec, req)

	if rec.Code != 200 {
		t.Fatalf("status = %d, want 200", rec.Code)
	}
	body := rec.Body.String()

	for _, want := range []string{
		"whiteboard_active_connections 1",
		"whiteboard_active_boards 1",
		"whiteboard_messages_received_total 1",
		"whiteboard_messages_delivered_total 3",
		"whiteboard_clients_kicked_total 1",
	} {
		if !strings.Contains(body, want) {
			t.Errorf("scrape output missing %q", want)
		}
	}
}
