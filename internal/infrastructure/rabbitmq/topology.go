package rabbitmq

import (
	"errors"

	amqp "github.com/rabbitmq/amqp091-go"
)

// RunTopology declares the durable direct exchange and work queue on the
// supplied channel. The queue has no message TTL: a backlog waits in the queue
// instead of expiring. Its caller owns the channel and connection lifetime.
func RunTopology(ch *amqp.Channel, queue string) error {
	if ch == nil {
		return errors.New("topology channel is required")
	}
	if err := ch.ExchangeDeclare(DefaultExchange, "direct", true, false, false, false, nil); err != nil {
		return err
	}
	if _, err := ch.QueueDeclare(queue, true, false, false, false, nil); err != nil {
		return err
	}
	return ch.QueueBind(queue, queue, DefaultExchange, false, nil)
}
