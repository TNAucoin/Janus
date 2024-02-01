package mq

import (
	"context"
	"errors"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
)

type RabbitClient struct {
	conn   *amqp.Connection
	ch     map[string]*amqp.Channel
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
	chmap := make(map[string]*amqp.Channel)
	ch, err := conn.Channel()
	if err != nil {
		return RabbitClient{}, err
	}
	// Puts the channel into confirm mode
	if err := ch.Confirm(false); err != nil {
		return RabbitClient{}, err
	}
	chmap["default"] = ch
	return RabbitClient{
		conn:   conn,
		ch:     chmap,
		logger: logger,
	}, nil
}

func (rc RabbitClient) CreateChannel(name string, noWait bool) error {
	if _, ok := rc.ch[name]; ok {
		chanAlreadyExistsErr := errors.New("cannot create a channel with a name already in use")
		return chanAlreadyExistsErr
	} else {
		ch, err := rc.conn.Channel()
		if err != nil {
			rc.logger.Err(err).Msgf("failed to create new channel: %s", name)
			return err
		}
		if !noWait {
			if err = ch.Confirm(noWait); err != nil {
				rc.logger.Err(err).Msgf("failed to put channel into confirm mode: %s", name)
				return err
			}
		}
		rc.ch[name] = ch
		rc.logger.Debug().Msgf("channel %s was created", name)
	}
	return nil
}

func (rc RabbitClient) Close() error {
	for _, ch := range rc.ch {
		err := ch.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

// CreateQueue creates a queue
func (rc RabbitClient) CreateQueue(chName, queueName string, durable, autodelete bool) error {
	ch, err := rc.getChFromName(chName)
	if err != nil {
		return errors.New(fmt.Sprintf("ch: %s could not be found", chName))
	}
	_, err = ch.QueueDeclare(queueName, durable, autodelete, false, false, nil)
	if err != nil {
		return err
	}
	return nil
}

// CreateBinding creates a binding between a queue and an exchange
func (rc RabbitClient) CreateBinding(chName, name, binding, exchange string) error {
	ch, err := rc.getChFromName(chName)
	if err != nil {
		return err
	}
	return ch.QueueBind(name, binding, exchange, false, nil)
}

func (rc RabbitClient) Send(ctx context.Context, chName, exchange, routingKey string, options amqp.Publishing) error {
	ch, err := rc.getChFromName(chName)
	if err != nil {
		return err
	}
	confirmation, err := ch.PublishWithDeferredConfirmWithContext(ctx,
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
func (rc RabbitClient) Consume(chName, queue, consumer string, autoAck bool) (<-chan amqp.Delivery, error) {
	ch, err := rc.getChFromName(chName)
	if err != nil {
		return nil, err
	}
	return ch.Consume(queue, consumer, autoAck, false, false, false, nil)
}

func (rc RabbitClient) getChFromName(chName string) (*amqp.Channel, error) {
	if ch, ok := rc.ch[chName]; ok {
		return ch, nil
	} else {
		chNotFoundErr := errors.New(fmt.Sprintf("failed to find ch: %s", chName))
		return nil, chNotFoundErr
	}
}
