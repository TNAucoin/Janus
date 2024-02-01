package main

import (
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/dynamo"
	"github.com/tnaucoin/Janus/internal/mq"
	"os"
	"os/signal"
	"sync"
)

var (
	localEnvFile = "local.env"
	workersCount = 10
	chName       = "job"
)

func main() {
	_ = godotenv.Load(localEnvFile)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	conf := config.New()
	ddbclient, err := dynamo.New(*conf, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create ddb client")
	}
	conn, err := mq.ConnectRabbitMQ(conf.MQ.Protocol, conf.MQ.User, conf.MQ.Password, conf.MQ.Host, conf.MQ.VHost, conf.MQ.Port)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to rabbitmq")
	}
	rmqc, err := mq.NewRabbitMQClient(conn, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create client")
	}
	if err := rmqc.CreateChannel(chName, true); err != nil {
		logger.Fatal().Msg("failed to create channel")
	}
	messageBus, err := rmqc.Consume(chName, "job_created", "worker-service", false)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to consume messages")
	}
	workers := make(chan struct{}, workersCount)
	messagesInProgress := &sync.WaitGroup{}
	shutDown := make(chan struct{})

	go func() {
		osSignals := make(chan os.Signal, 1)
		signal.Notify(osSignals, os.Interrupt)
		<-osSignals
		close(shutDown)
	}()

	go func() {
		for {
			select {
			case <-shutDown:
				return
			case message, ok := <-messageBus:
				if !ok {
					return
				}
				workers <- struct{}{} //block if no free workers
				messagesInProgress.Add(1)
				go func(msg amqp.Delivery) {
					recordID := doWork(msg, logger)
					if recordID != "" {
						if err := ddbclient.DequeueRecord(recordID); err != nil {
							logger.Err(err).Msgf("failed to dequeue record %s", recordID)
						} else {
							logger.Debug().Msgf("dequeued: %s", recordID)
						}
					} else {
						logger.Error().Msg("null record")
					}
					<-workers // release the worker block
					messagesInProgress.Done()
				}(message)
			}
		}
	}()

	logger.Info().Msg("consuming messages...")
	<-shutDown                // block until interrupt signal is received
	messagesInProgress.Wait() // block until all messages have been processed
}

func doWork(message amqp.Delivery, logger zerolog.Logger) string {

	if err := message.Ack(false); err != nil {
		logger.Err(err).Msgf("failed to ack message: %s", message.MessageId)
		return ""
	}
	logger.Info().Str("op", "message-ack").Str("message-id", message.MessageId).Msg("succcessfully acknowledged message")
	return message.MessageId
}
