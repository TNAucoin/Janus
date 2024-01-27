package main

import (
	"github.com/joho/godotenv"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/dynamo"
	"log"
)

var (
	localEnvFile = "local.env"
)

func main() {
	// Load the local environment variables
	_ = godotenv.Load(localEnvFile)

	conf := config.New()
	ddbclient, err := dynamo.New(conf.DB)
	if err != nil {
		log.Fatalf("failed to create ddb client: %v", err)
	}
	// If running locally create the table if it doesn't exist
	if conf.App.Env == "development" {
		err = dynamo.CreateLocalTable(ddbclient)
		if err != nil {
			log.Fatalf("failed to create the ddb table: %v", err)
		}
	}
}
