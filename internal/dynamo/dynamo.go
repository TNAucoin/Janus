package dynamo

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/tnaucoin/Janus/config"
)

type DDBConnection struct {
	Client    *dynamodb.Client
	Region    string
	TableName string
}

// New is a function that creates a new instance of DDBConnection, which represents a connection to Amazon DynamoDB.
// It takes four parameters:
// - tableName: a string representing the name of the table to connect to in DynamoDB.
// - region: a string representing the AWS region in which the table is located.
// - hostUrl: a string representing the hostname or IP address of the DynamoDB server.
// - port: an integer representing the port number on which the DynamoDB server is listening.
// The function returns a pointer to DDBConnection and an error if any occurred during creation.
func New(conf config.ConfDB) (*DDBConnection, error) {
	var db *dynamodb.Client
	url, err := createDynamoDbURL(conf.Host, conf.Port)
	if err != nil {
		return nil, err
	}
	cfg, err := createLocalConfig(conf.Region, url)
	if err != nil {
		return nil, err
	}
	db = dynamodb.NewFromConfig(cfg)
	return &DDBConnection{
		Client:    db,
		Region:    conf.Region,
		TableName: conf.TableName,
	}, nil
}
