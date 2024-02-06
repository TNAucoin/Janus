package main

import (
	"context"
	"fmt"
	"github.com/danielgtaylor/huma/v2"
	"github.com/danielgtaylor/huma/v2/adapters/humachi"
	"github.com/go-chi/chi/v5"
	"github.com/joho/godotenv"
	"github.com/rs/zerolog"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/dynamo"
	"github.com/tnaucoin/Janus/models/QueueRecord"
	"net/http"
	"os"
)

var (
	localEnvFile = "local.env"
	workersCount = 10
	chName       = "job"
	baseApiPath  = "/api/v1"
)

type CreateJobRecordInput struct {
	Body struct {
		Priority string `json:"priority,required" enum:"NONE,LOW,MEDIUM,HIGH"`
	}
}
type CreateJobRecordOutput struct {
	Body struct {
		JobID string `json:"job_id" example:"803d73fa-1dd7-41c4-8b03-5125f1b5446c" doc:"The resulting job-id"`
	}
}
type JobEnqueueInput struct {
	Body struct {
		JobIDs         []string `json:"job_ids,required" example:"dc08db82-ca8b-44bc-be55-c5b36cd774f4,803d73fa-1dd7-41c4-8b03-5125f1b5446c" doc:"The job_ids to enqueue"`
		QueuePartition int      `json:"queue_partition,required" example:"1" doc:"The queue partition to enqueue the record into" default:"1"`
	}
}

type JobEnqueueOutput struct{}

func main() {
	logger, _, ddbConnection := Init()
	router := chi.NewMux()
	api := humachi.New(router, huma.DefaultConfig("Janus", "1.0.0"))
	huma.Register(api, huma.Operation{
		OperationID:   "create-job-record",
		Summary:       "Create a Janus Job Record Entry",
		Method:        http.MethodPost,
		Path:          fmt.Sprintf("%s/job", baseApiPath),
		DefaultStatus: http.StatusCreated,
	}, func(ctx context.Context, input *CreateJobRecordInput) (*CreateJobRecordOutput, error) {
		resp := &CreateJobRecordOutput{}
		var priority QueueRecord.QPriority
		switch input.Body.Priority {
		case "NONE":
			priority = QueueRecord.NONE
		case "LOW":
			priority = QueueRecord.LOW
		case "MEDIUM":
			priority = QueueRecord.MEDIUM
		case "HIGH":
			priority = QueueRecord.HIGH
		}
		logger.Info().Str("Priority", input.Body.Priority).Int64("Value", int64(priority)).Msg("")
		record := QueueRecord.NewQRecord(priority)
		if err := ddbConnection.AddRecord(record); err != nil {
			logger.Err(err).Msg("failed to add record to dynamo")
			return nil, err
		}
		resp.Body.JobID = record.Id
		return resp, nil
	})
	huma.Register(api, huma.Operation{
		OperationID:   "enqueue-job-records",
		Summary:       "Enqueue Janus Job Records",
		Method:        http.MethodPost,
		Path:          fmt.Sprintf("%s/enqueue", baseApiPath),
		DefaultStatus: http.StatusAccepted,
	}, func(ctx context.Context, input *JobEnqueueInput) (*JobEnqueueOutput, error) {
		resp := &JobEnqueueOutput{}
		for _, jobId := range input.Body.JobIDs {
			if err := ddbConnection.EnqueueRecord(jobId, input.Body.QueuePartition); err != nil {
				logger.Err(err).Msgf("failed to enqueue record: %s", jobId)
				return nil, err
			}
			logger.Debug().Str("enqueued", jobId).Int("queue-partiion", input.Body.QueuePartition).Msg("")
		}
		return resp, nil
	})
	logger.Info().Msg("api running.")
	http.ListenAndServe("127.0.0.1:8001", router)
}
func Init() (zerolog.Logger, *config.Conf, *dynamo.DDBConnection) {
	_ = godotenv.Load(localEnvFile)
	zerolog.TimeFieldFormat = zerolog.TimeFormatUnix
	zerolog.SetGlobalLevel(zerolog.DebugLevel)
	logger := zerolog.New(zerolog.ConsoleWriter{Out: os.Stderr}).With().Timestamp().Logger()
	conf := config.New()
	ddbclient, err := dynamo.New(*conf, logger)
	if err != nil {
		logger.Fatal().Err(err)
	}
	if conf.App.Env == "development" {
		err = dynamo.CreateLocalTable(ddbclient)
		if err != nil {
			logger.Fatal().Err(err).Msg("failed to create the ddb table")
		}
	}
	return logger, conf, ddbclient
}
