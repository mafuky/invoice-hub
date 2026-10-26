package mq

import (
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func TestDrainReturns_ClosedChannelReturns(t *testing.T) {
	for _, buffered := range []bool{false, true} {
		t.Run(map[bool]string{false: "empty", true: "buffered"}[buffered], func(t *testing.T) {
			ch := make(chan amqp.Return, 2)
			if buffered {
				ch <- amqp.Return{MessageId: "old"}
				ch <- amqp.Return{MessageId: "current"}
			}
			close(ch)
			done := make(chan struct{})
			go func() { drainReturns(ch); close(done) }()
			select {
			case <-done:
				if len(ch) != 0 {
					t.Fatal("buffered returns were not drained")
				}
			case <-time.After(time.Second):
				t.Fatal("drainReturns did not finish on a closed channel")
			}
		})
	}
}

func TestFindReturn_ClosedChannelReturns(t *testing.T) {
	for _, tc := range []struct {
		name, id string
		messages []string
		want     bool
	}{
		{"empty", "current", nil, false},
		{"empty_id", "", nil, false},
		{"nonmatching", "current", []string{"old"}, false},
		{"matching", "current", []string{"old", "current"}, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ch := make(chan amqp.Return, len(tc.messages))
			for _, id := range tc.messages {
				ch <- amqp.Return{MessageId: id, ReplyCode: 312}
			}
			close(ch)
			type result struct {
				r  amqp.Return
				ok bool
			}
			done := make(chan result, 1)
			go func() { r, ok := findReturn(ch, tc.id); done <- result{r, ok} }()
			select {
			case got := <-done:
				if got.ok != tc.want || got.ok && (got.r.MessageId != tc.id || got.r.ReplyCode != 312) {
					t.Fatalf("findReturn = %+v, want match %v", got, tc.want)
				}
			case <-time.After(time.Second):
				t.Fatal("findReturn did not finish on a closed channel")
			}
		})
	}
}
