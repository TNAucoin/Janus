package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/mq"
	"os"
)

var (
	localEnvFile = "local.env"
)

func main() {
	_ = godotenv.Load(localEnvFile)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	conf := config.New()
	conn, err := mq.ConnectRabbitMQ(conf.MQ.Protocol, conf.MQ.User, conf.MQ.Password, conf.MQ.Host, conf.MQ.VHost, conf.MQ.Port)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to rabbitmq")
	}
	rmqc, err := mq.NewRabbitMQClient(conn)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create client")
	}
	messageBus, err := rmqc.Consume("job_created", "worker-service", false)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to consume messages")
	}

	var blocking chan struct{}

	go func() {
		for message := range messageBus {
			logger.Debug().Str("op", "message-received").Str("value", fmt.Sprintf("%v", message)).Msg("")
			if err := message.Ack(false); err != nil {
				logger.Err(err).Msgf("failed to ack message: %s", message.MessageId)
				continue
			}
			logger.Info().Str("op", "message-ack").Str("message-id", message.MessageId).Msg("succcessfully acknowledged message")
		}
	}()

	logger.Info().Msg("consuming messages...")
	<-blocking
}
