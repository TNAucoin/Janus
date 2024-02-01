package mq

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type RabbitClient struct {
	conn   *amqp.Connection
	ch     *amqp.Channel
	logger zerolog.Logger
}

func ConnectRabbitMQ(protocol, username, password, host, vhost string, port int) (*amqp.Connection, error) {
	conn, err := amqp.Dial(fmt.Sprintf("%s://%s:%s@%s:%d/%s", protocol, username, password, host, port, vhost))
	if err != nil {
		return nil, err
	}
	return conn, nil
}

// NewRabbitMQClient TODO: instead of creating a channel, we should make a new channel function, that accepts a channel input
func NewRabbitMQClient(conn *amqp.Connection, logger zerolog.Logger) (RabbitClient, error) {
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}
	// Puts the channel into confirm mode
	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}
	return RabbitClient{
		conn:   conn,
		ch:     ch,
		logger: logger,
	}, nil
}

func (rc RabbitClient) Close() error {
	return rc.ch.Close()
}

// CreateQueue creates a queue
func (rc RabbitClient) CreateQueue(queueName string, durable, autodelete bool) error {
	_, err := rc.ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	return err
}

// CreateBinding creates a binding between a queue and an exchange
func (rc RabbitClient) CreateBinding(name, binding, exchange string) error {
	return rc.ch.QueueBind(name, binding, exchange, false, nil)
}

func (rc RabbitClient) Send(ctx context.Context, exchange, routingKey string, options amqp.Publishing) error {
	confirmation, err := rc.ch.PublishWithDeferredConfirmWithContext(ctx,
		exchange,
		routingKey,
		true, // Message must return an error, this will make a failed send bounce back
		false,
		options,
	)
	if err != nil {
		return err
	}
	rc.logger.Debug().Msgf("server confirmation: %d", confirmation.DeliveryTag)
	return nil
}
func (rc RabbitClient) Consume(queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	return rc.ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}
