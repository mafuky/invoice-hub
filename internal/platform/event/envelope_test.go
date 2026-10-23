package event

import (
	"encoding/json"
	"testing"

	"github.com/google/uuid"
)

func TestEnvelope_NewAndDecode(t *testing.T) {
	e, err := New("invoice.task.created", "inv-1", 3, map[string]string{"a": "b"}, "tr-1")
	if err != nil {
		t.Fatal(err)
	}
	id, err := uuid.Parse(e.EventID)
	if err != nil || id.Version() != 7 {
		t.Fatalf("event id %q is not a UUIDv7 (%v)", e.EventID, err)
	}

	var got map[string]string
	if err := e.Decode(&got); err != nil {
		t.Fatal(err)
	}
	if got["a"] != "b" {
		t.Fatalf("decoded %v", got)
	}

	raw, err := json.Marshal(e)
	if err != nil {
		t.Fatal(err)
	}
	var back Envelope
	if err := json.Unmarshal(raw, &back); err != nil {
		t.Fatal(err)
	}
	if back.EventID != e.EventID || back.Type != e.Type || back.AggregateID != e.AggregateID ||
		back.Version != e.Version || back.TraceID != e.TraceID ||
		!back.OccurredAt.Equal(e.OccurredAt) || string(back.Payload) != string(e.Payload) {
		t.Fatalf("round trip mismatch: %+v vs %+v", back, e)
	}
}
