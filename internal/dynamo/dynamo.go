package dynamo

import (
	"context"
	"fmt"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/models/QueueRecord"
)

type DDBConnection struct {
	Client    *dynamodb.Client
	Region    string
	TableName string
	IndexName string
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
		IndexName: conf.IndexName,
	}, nil
}

func (ddbc *DDBConnection) AddRecord(record *QueueRecord.QRecord) error {
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return err
	}
	_, err = ddbc.Client.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(ddbc.TableName),
		Item:      item,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ddbc *DDBConnection) EnqueueRecord(id string) error {
	record, err := ddbc.getRecord(id)
	if err != nil {
		return err
	}
	fmt.Printf("old version: %d \n", record.SystemInfo.Version)
	upd := expression.
		Set(expression.Name("queued"), expression.Value(aws.Int64(1))).
		Set(expression.Name("system_info.queued"), expression.Value(aws.Int64(1))).
		Add(expression.Name("system_info.version"), expression.Value(aws.Int64(1)))
	cond := expression.Equal(
		expression.Name("system_info.version"),
		expression.Value(aws.Int64(record.SystemInfo.Version)),
	)
	expr, err := expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	if err != nil {
		return err
	}
	_, err = ddbc.Client.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                       QueueRecord.IdToKeyExpr(id),
		TableName:                 aws.String(ddbc.TableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ddbc *DDBConnection) getRecord(id string) (*QueueRecord.QRecord, error) {
	resp, err := ddbc.Client.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:            QueueRecord.IdToKeyExpr(id),
		TableName:      aws.String(ddbc.TableName),
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		return nil, err
	}
	var record = QueueRecord.QRecord{}
	err = attributevalue.UnmarshalMap(resp.Item, &record)
	if err != nil {
		return nil, err
	}
	return &record, nil
}
