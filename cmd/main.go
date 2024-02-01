package main

import (
	"context"
	"fmt"
	"github.com/joho/godotenv"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/rs/zerolog"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/dynamo"
	"github.com/tnaucoin/Janus/internal/mq"
	"github.com/tnaucoin/Janus/models/QueueRecord"
	"os"
	"strconv"
	"time"
)

var (
	localEnvFile = "local.env"
)

func main() {
	// Load the local environment variables
	_ = godotenv.Load(localEnvFile)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	conf := config.New()
	conn, err := mq.ConnectRabbitMQ(conf.MQ.Protocol, conf.MQ.User, conf.MQ.Password, conf.MQ.Host, conf.MQ.VHost, conf.MQ.Port)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to connect to rabbitmq")
	}
	defer conn.Close()
	rabbitClient, err := mq.NewRabbitMQClient(conn, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("failed to create the RabbitMQ client")
	}
	logger.Info().Msg("connected to RabbitMQ")
	defer rabbitClient.Close()
	ddbclient, err := dynamo.New(*conf, logger)
	if err != nil {
		logger.Fatal().Err(err)
	}
	// If running locally create the table if it doesn't exist
	if conf.App.Env == "development" {
		err = dynamo.CreateLocalTable(ddbclient)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to create the ddb table")
		}
		if err := rabbitClient.CreateQueue("job_created", true, false); err != nil {
			logger.Fatal().Err(err).Msg("")
		}
		if err := rabbitClient.CreateQueue("job_test", false, true); err != nil {
			logger.Fatal().Err(err).Msg("")
		}
		if err := rabbitClient.CreateBinding("job_created", "job.created.*", "job_events"); err != nil {
			logger.Fatal().Err(err).Msg("")
		}
		if err := rabbitClient.CreateBinding("job_test", "job.*", "job_events"); err != nil {
			logger.Fatal().Err(err).Msg("")
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		var numJobs = 5
		for i := 0; i < numJobs; i++ {
			record := CreateRecord(ddbclient, logger)
			Enqueue(record.Id, 1, ddbclient, logger)
		}
		for i := 0; i < numJobs; i++ {
			r, err := ddbclient.Peek(1)
			if err != nil {
				logger.Fatal().Msg("Failed to peek, for events")
			}
			if err := rabbitClient.Send(ctx, "job_events", fmt.Sprintf("job.created.%d", i), amqp.Publishing{
				ContentType:  "text/plain",
				DeliveryMode: amqp.Transient,
				Body:         []byte("Some job message"),
				MessageId:    r.Id,
				Priority:     1,
			}); err != nil {
				logger.Fatal().Err(err).Msg("")
			}
		}
		time.Sleep(30 * time.Second)
		//r1 := CreateRecord(ddbclient, logger)
		//Enqueue(r1.Id, 1, ddbclient, logger)
		//r2 := CreateRecord(ddbclient, logger)
		//Enqueue(r2.Id, 1, ddbclient, logger)
		//p1, _ := ddbclient.Peek(1)
		//if p1 != nil {
		//	logger.Debug().Str("op", "peek-result").Str("record-id", p1.Id).Msg("")
		//}
		//Dequeue(p1.Id, ddbclient)

	}
}

func CreateRecord(ddb *dynamo.DDBConnection, logger zerolog.Logger) QueueRecord.QRecord {
	q := QueueRecord.NewQRecord()
	if err := ddb.AddRecord(q); err != nil {
		logger.Fatal().Err(err).Msg("failed to add message to ddb")
	}
	logger.Info().Str("op", "create-record").Str("record-id", q.Id).Str("record-id", q.Id).Msg("")
	return *q
}

func Enqueue(id string, priority int, ddb *dynamo.DDBConnection, logger zerolog.Logger) {
	err := ddb.EnqueueRecord(id, priority)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to enqueue record")
	}
	logger.Info().Str("op", "enqueue").Str("record-id", id).Str("priority", strconv.Itoa(priority)).Msg("")
}

func Dequeue(id string, ddb *dynamo.DDBConnection, logger zerolog.Logger) {
	err := ddb.DequeueRecord(id)
	if err != nil {
		logger.Fatal().Err(err).Str("record-id", id).Msg("failed to dequeue record")
	}
	logger.Info().Str("op", "dequeue").Str("record-id", id).Msg("")
}
