package audit

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"os"
	"strings"
	"time"

	"github.com/google/uuid"
)

// Event has a stable ID that downstream sinks use for idempotent ingestion.
// Delivery is intentionally at-least-once, never best-effort fire-and-forget.
type Event struct {
	ID            uuid.UUID       `json:"id"`
	ProjectID     uuid.UUID       `json:"project_id"`
	EventType     string          `json:"event_type"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   uuid.UUID       `json:"aggregate_id"`
	Payload       json.RawMessage `json:"payload"`
	CreatedAt     time.Time       `json:"created_at"`
}
type Sink interface {
	Publish(context.Context, Event) error
}
type noop struct{}

func (noop) Publish(context.Context, Event) error { return nil }

type HTTPSink struct {
	endpoint string
	token    string
	client   *http.Client
}

func FromEnv() (Sink, error) {
	raw := strings.TrimSpace(os.Getenv("TASK_AUDIT_SINK_URL"))
	if raw == "" {
		return noop{}, nil
	}
	u, err := url.ParseRequestURI(raw)
	if err != nil || u.Scheme != "https" && u.Scheme != "http" {
		return nil, fmt.Errorf("TASK_AUDIT_SINK_URL must be an absolute HTTP(S) URL")
	}
	return &HTTPSink{endpoint: raw, token: strings.TrimSpace(os.Getenv("TASK_AUDIT_SINK_TOKEN")), client: &http.Client{Timeout: 10 * time.Second}}, nil
}
func (s *HTTPSink) Publish(ctx context.Context, event Event) error {
	body, err := json.Marshal(event)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.endpoint, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Audit-Event-ID", event.ID.String())
	if s.token != "" {
		req.Header.Set("Authorization", "Bearer "+s.token)
	}
	res, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()
	if res.StatusCode < 200 || res.StatusCode > 299 {
		b, _ := io.ReadAll(io.LimitReader(res.Body, 4096))
		return fmt.Errorf("audit sink returned %s: %s", res.Status, strings.TrimSpace(string(b)))
	}
	return nil
}
