package mq

import (
	"context"
	"errors"
	"fmt"
	"github.com/cenkalti/backoff"
	"github.com/gammazero/workerpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"strings"
	"sync"
	"time"
)

type MessageBody struct {
	Data []byte
	Type string
}

type Message struct {
	Queue         string
	ReplyTo       string
	ContentType   string
	CorrelationID string
	Priority      uint8
	Body          MessageBody
}

type Connection struct {
	mu       sync.Mutex
	name     string
	conn     *amqp.Connection
	channel  *amqp.Channel
	exchange string
	queues   []string
	err      chan error
}

var (
	connectionPool = make(map[string]*Connection)
)

func CloseAllConnections() {
	for _, connection := range connectionPool {
		if connection.conn != nil && !connection.conn.IsClosed() {
			connection.conn.Close()
		}
	}
}

// NewConnection creates a new connection to the message queue with the specified name, exchange, and queues.
// If a connection with the same name already exists in the connection pool, it returns that connection.
// Otherwise, it creates a new Connection object and adds it to the connection pool.
// The Connection object is initialized with the provided exchange, queues, and an error channel.
// It then connects to the message queue, binds the queues, and returns the Connection object.
func NewConnection(name, exchange string, queues []string) *Connection {
	if c, ok := connectionPool[name]; ok {
		return c
	}
	c := &Connection{
		exchange: exchange,
		queues:   queues,
		err:      make(chan error),
	}
	connectionPool[name] = c
	return c
}

// GetConnection retrieves the connection from the connection pool based on the specified name.
// It returns the *Connection object corresponding to the name.
func GetConnection(name string) *Connection {
	return connectionPool[name]
}

// SetError sets the error value in the `Connection` struct.
// It takes an `err` of type `error` as an argument.
// It acquires a lock on the `Connection` struct, sets the error value, and releases the lock.
// Usage Example:
//
//	c.SetError(errors.New("connection Closed"))
func (c *Connection) SetError(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.err <- err
}

// Connect creates a connection to RabbitMQ and initializes a channel.
// It also declares the exchange.
// Returns an error if any step fails.
func (c *Connection) Connect() error {
	var err error
	c.conn, err = amqp.Dial("amqp://janus:password@localhost:5672/janus")
	if err != nil {
		return fmt.Errorf("error creating rmq connection: %s", err)
	}
	go func() {
		<-c.conn.NotifyClose(make(chan *amqp.Error))
		c.SetError(errors.New("connection Closed"))
	}()
	c.channel, err = c.conn.Channel()
	if err != nil {
		return fmt.Errorf("channel: %s", err)
	}

	if err := c.channel.ExchangeDeclare(
		c.exchange,
		"topic",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return fmt.Errorf("error in Exchange Declare: %s", err)
	}
	return nil
}

// BindQueue binds the queues specified in `c.queues` to the exchange specified in `c.exchange`.
// It declares each queue, and if a custom routing key is defined in the queue name, it binds the queue with that routing key to the exchange.
// If no custom routing key is defined, it binds the queue with the default routing key "default" to the exchange.
// Returns an error if any step fails.
func (c *Connection) BindQueue() error {
	for _, q := range c.queues {
		keys := strings.Split(q, "::")
		fmt.Println(keys)
		if _, err := c.channel.QueueDeclare(keys[0], true, false, false, false, nil); err != nil {
			return fmt.Errorf("error declaring the queue %s", err)
		}
		// If this key contained a custom routing key
		if len(keys) > 1 {
			if err := c.channel.QueueBind(keys[0], keys[1], c.exchange, false, nil); err != nil {
				return fmt.Errorf("queue bind error: %s", err)
			}
		} else {
			// no custom routing key was defined, used default key
			if err := c.channel.QueueBind(keys[0], "default", c.exchange, false, nil); err != nil {
				return fmt.Errorf("queue bind error: %s", err)
			}
		}
		fmt.Printf("Q-Connected: %s\n", keys[0])
	}
	return nil
}

// Reconnect tries to reconnect the connection by calling Connect().
// If Connect returns an error, it returns the same error.
// If Connect is successful, it calls BindQueue().
// If BindQueue returns an error, it returns the same error.
// If BindQueue is successful, it returns nil.
// Reconnect should be called whenever there is a need to establish a new connection and bind the queues again.
func (c *Connection) Reconnect() error {
	if err := c.Connect(); err != nil {
		return err
	}
	if err := c.BindQueue(); err != nil {
		return err
	}
	return nil
}

// Consume consumes messages from the RabbitMQ queues specified in the `Connection` instance.
// It returns a map where the keys are the queue names and the values are channels of `amqp.Delivery`.
// If there is an error while consuming from any of the queues, it returns the error.
func (c *Connection) Consume() (map[string]<-chan amqp.Delivery, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	m := make(map[string]<-chan amqp.Delivery)
	for _, q := range c.queues {
		keys := strings.Split(q, ":")
		deliveries, err := c.channel.Consume(keys[0], "", false, false, false, false, nil)
		if err != nil {
			return nil, err
		}
		m[q] = deliveries
	}
	return m, nil
}

func (c *Connection) Publish(m Message) error {
	select {
	case err := <-c.err:
		if err != nil {
			c.Reconnect()
		}
	default:
	}
	p := amqp.Publishing{
		Headers:       amqp.Table{"type": m.Body.Type},
		DeliveryMode:  amqp.Persistent,
		ContentType:   m.ContentType,
		CorrelationId: m.CorrelationID,
		Body:          m.Body.Data,
		Priority:      m.Priority,
	}
	if err := c.channel.PublishWithContext(context.Background(), c.exchange, m.Body.Type, false, false, p); err != nil {
		return err
	}
	return nil
}

// HandleConsumedDeliveries accepts a queue name, a delivery channel, and a function to handle the deliveries.
// It creates a workerpool with a pool size of 5.
// The function continuously submits the delivery handling function to the workerpool.
// If an error is received from the error channel, it stops accepting new entries in the workerpool and attempts to reconnect.
// It uses an exponential backoff policy to retry the reconnect process.
// If the reconnect process fails after the maximum number of attempts, it stops the workerpool and panics with the last retry error.
//
// Parameters:
// - q: the name of the queue to consume from
// - delivery: the channel for receiving deliveries
// - fn: the function to handle the deliveries
//
// Example usage:
//
//	c.HandleConsumedDeliveries("my-queue", deliveries, handleDelivery)
func (c *Connection) HandleConsumedDeliveries(q string, delivery <-chan amqp.Delivery, fn func(*Connection, string, <-chan amqp.Delivery)) {
	wp := workerpool.New(5)
	for {

		wp.Submit(func() {
			fn(c, q, delivery)
		})

		if err := <-c.err; err != nil {
			// stops new entries from appearing in pool
			// once drained it will return.
			wp.StopWait() // stop the workerpool for retry
			policy := c.createBackOffPolicy()
			start := time.Now()
			attempts := 0
			fmt.Println("connection lost attempting reconnect...")
			retryErr := backoff.Retry(func() error {
				time.Sleep(time.Since(start))
				fmt.Printf("Reconnecting: Attempt:%d, Waiting:%f seconds\n", attempts, time.Since(start).Seconds())
				err := c.Reconnect()
				if err != nil {
					attempts++
					return err
				}
				deliveries, err := c.Consume()
				if err != nil {
					attempts++
					return err
				}
				delivery = deliveries[q]
				wp = workerpool.New(5)
				return nil
			}, policy)

			if retryErr != nil {
				wp.Stop()
				panic(retryErr)
			}
		}
	}
}

func (c *Connection) createBackOffPolicy() backoff.BackOff {
	policy := backoff.NewExponentialBackOff()
	policy.MaxElapsedTime = 60 * time.Second
	policy.MaxInterval = 2 * time.Second
	return policy
}
