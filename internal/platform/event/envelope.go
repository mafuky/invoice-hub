// Package event defines the envelope carried by every message on the bus.
package event

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type Envelope struct {
	EventID     string          `json:"event_id"`
	Type        string          `json:"type"`
	OccurredAt  time.Time       `json:"occurred_at"`
	AggregateID string          `json:"aggregate_id"`
	Version     int             `json:"version"`
	TraceID     string          `json:"trace_id"`
	Payload     json.RawMessage `json:"payload"`
}

func New(typ, aggregateID string, version int, payload any, traceID string) (Envelope, error) {
	id, err := uuid.NewV7()
	if err != nil {
		return Envelope{}, fmt.Errorf("event: new id: %w", err)
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return Envelope{}, fmt.Errorf("event: marshal payload: %w", err)
	}
	return Envelope{
		EventID:     id.String(),
		Type:        typ,
		OccurredAt:  time.Now().UTC(),
		AggregateID: aggregateID,
		Version:     version,
		TraceID:     traceID,
		Payload:     raw,
	}, nil
}

func (e Envelope) Decode(v any) error {
	return json.Unmarshal(e.Payload, v)
}
