package mq

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	amqp "github.com/rabbitmq/amqp091-go"

	"github.com/mafuky/invoice-hub/internal/platform/event"
)

func TestRunHandler_PanicBecomesError(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	err := runHandler(context.Background(), log, func(context.Context, event.Envelope) error {
		var m map[string]int
		m["delivery"] = 1
		return nil
	}, event.Envelope{EventID: "e-1"})
	if err == nil || !strings.Contains(err.Error(), "panic") {
		t.Fatalf("panic not converted to error: %v", err)
	}
}

func TestAttemptOf_HeaderTypes(t *testing.T) {
	cases := []struct {
		v    any
		want int
	}{
		{nil, 0}, {int32(2), 2}, {int64(3), 3}, {uint16(1), 1}, {uint32(2), 2},
		{"3", 3}, {"x", 0}, {int32(-4), 0},
	}
	for _, c := range cases {
		d := amqp.Delivery{Headers: amqp.Table{headerAttempt: c.v}}
		if got := attemptOf(d); got != c.want {
			t.Errorf("attemptOf(%#v) = %d, want %d", c.v, got, c.want)
		}
	}
}
