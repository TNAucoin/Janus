package main

import (
	"fmt"
	"github.com/joho/godotenv"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/internal/dynamo"
	"github.com/tnaucoin/Janus/models/QueueRecord"
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
		r1 := CreateRecord(ddbclient)
		Enqueue(r1.Id, 1, ddbclient)
		r2 := CreateRecord(ddbclient)
		Enqueue(r2.Id, 1, ddbclient)
		p1, _ := ddbclient.Peek(1)
		fmt.Printf("RETURN TO CLIENT: ID: %s\n", p1.Id)
		//Dequeue(p1.Id, ddbclient)

	}
}

func CreateRecord(ddb *dynamo.DDBConnection) QueueRecord.QRecord {
	q := QueueRecord.NewQRecord()
	if err := ddb.AddRecord(q); err != nil {
		log.Fatalf("ddb error: %v", err)
	}
	fmt.Printf("Enqueued: %s\n", q.Id)
	return *q
}

func Enqueue(id string, priority int, ddb *dynamo.DDBConnection) {
	err := ddb.EnqueueRecord(id, priority)
	if err != nil {
		log.Fatalf("failed to enqueue record: %s. %v\n", id, err)
	}
}

func Dequeue(id string, ddb *dynamo.DDBConnection) {
	err := ddb.DequeueRecord(id)
	if err != nil {
		log.Fatalf("failed to dequeue: %s. %v\n", id, err)
	}
	fmt.Printf("Dequeued: %s\n", id)
}
