package mq

import amqp "github.com/rabbitmq/amqp091-go"

// drainReturns discards returns left over from earlier publishes.
func drainReturns(ch <-chan amqp.Return) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

// findReturn reports whether the broker returned the message with the given id.
func findReturn(ch <-chan amqp.Return, messageID string) (amqp.Return, bool) {
	for {
		select {
		case r := <-ch:
			if r.MessageId == messageID {
				return r, true
			}
		default:
			return amqp.Return{}, false
		}
	}
}
