package dynamo

import (
	"context"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/attributevalue"
	"github.com/aws/aws-sdk-go-v2/feature/dynamodb/expression"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/rs/zerolog"
	"github.com/tnaucoin/Janus/config"
	"github.com/tnaucoin/Janus/models/QueueRecord"
	"github.com/tnaucoin/Janus/utils"
	"time"
)

type DDBConnection struct {
	dbClient   *dynamodb.Client
	region     string
	tableName  string
	indexName  string
	logger     zerolog.Logger
	maxRetries int
}

// New is a function that creates a new instance of DDBConnection, which represents a connection to Amazon DynamoDB.
// It takes four parameters:
// - tableName: a string representing the name of the table to connect to in DynamoDB.
// - region: a string representing the AWS region in which the table is located.
// - hostUrl: a string representing the hostname or IP address of the DynamoDB server.
// - port: an integer representing the port number on which the DynamoDB server is listening.
// The function returns a pointer to DDBConnection and an error if any occurred during creation.
func New(conf config.Conf, logger zerolog.Logger) (*DDBConnection, error) {
	var db *dynamodb.Client
	url, err := createDynamoDbURL(conf.DB.Host, conf.DB.Port)
	if err != nil {
		return nil, err
	}
	cfg, err := createLocalConfig(conf.DB.Region, url)
	if err != nil {
		return nil, err
	}
	db = dynamodb.NewFromConfig(cfg)
	return &DDBConnection{
		dbClient:   db,
		region:     conf.DB.Region,
		tableName:  conf.DB.TableName,
		indexName:  conf.DB.IndexName,
		logger:     logger,
		maxRetries: conf.Queue.MaxRetries,
	}, nil
}

func (ddbc *DDBConnection) AddRecord(record *QueueRecord.QRecord) error {
	item, err := attributevalue.MarshalMap(record)
	if err != nil {
		return err
	}
	_, err = ddbc.dbClient.PutItem(context.TODO(), &dynamodb.PutItemInput{
		TableName: aws.String(ddbc.tableName),
		Item:      item,
	})
	if err != nil {
		return err
	}
	return nil
}

func (ddbc *DDBConnection) EnqueueRecord(id string, priority int) error {
	record, err := ddbc.getRecord(id)
	timestamp := utils.GetCurrentTimeInMilliseconds()
	if err != nil {
		return err
	}
	upd := expression.
		Set(expression.Name("queued"), expression.Value(aws.Int(priority))).
		Set(expression.Name("system_info.queued"), expression.Value(aws.Int(priority))).
		Set(expression.Name("system_info.queue_added_timestamp"), expression.Value(timestamp)).
		Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
		Set(expression.Name("system_info.status"), expression.Value(aws.String(QueueRecord.QStatusToString[QueueRecord.Ready]))).
		Set(expression.Name("priority_timestamp"), expression.Value(record.Priority)).
		Set(expression.Name("system_info.priority_timestamp"), expression.Value(record.Priority)).
		Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(timestamp)).
		Add(expression.Name("system_info.version"), expression.Value(aws.Int64(1)))

	cond := expression.Equal(
		expression.Name("system_info.version"),
		expression.Value(aws.Uint(record.SystemInfo.Version)),
	)

	expr, err := expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	if err != nil {
		return err
	}
	_, err = ddbc.dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                       QueueRecord.IdToKeyExpr(id),
		TableName:                 aws.String(ddbc.tableName),
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

func (ddbc *DDBConnection) DequeueRecord(id string) error {
	record, err := ddbc.getRecord(id)
	if err != nil {
		return err
	}
	timestamp := utils.GetCurrentTimeInMilliseconds()
	upd := expression.
		Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
		Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(timestamp)).
		Add(expression.Name("system_info.version"), expression.Value(aws.Uint(1))).
		Set(expression.Name("system_info.status"), expression.Value(aws.String(QueueRecord.QStatusToString[QueueRecord.Done]))).
		Set(expression.Name("system_info.queue_remove_timestamp"), expression.Value(timestamp)).
		Set(expression.Name("system_info.queued"), expression.Value(-1)).
		Remove(expression.Name("queued"))

	cond := expression.Equal(
		expression.Name("system_info.version"),
		expression.Value(aws.Uint(record.SystemInfo.Version)),
	)

	expr, err := expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	if err != nil {
		return err
	}
	_, err = ddbc.dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                       QueueRecord.IdToKeyExpr(id),
		TableName:                 aws.String(ddbc.tableName),
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

func (ddbc *DDBConnection) Peek(priority int64) (*QueueRecord.QRecord, error) {
	cond := expression.Equal(
		expression.Name("queued"),
		expression.Value(aws.Int64(priority)),
	)
	expr, err := expression.NewBuilder().WithCondition(cond).Build()
	resp, err := ddbc.dbClient.Query(context.TODO(), &dynamodb.QueryInput{
		TableName:                 aws.String(ddbc.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		IndexName:                 aws.String(ddbc.indexName),
		KeyConditionExpression:    expr.Condition(),
		Limit:                     aws.Int32(250),
		ScanIndexForward:          aws.Bool(true),
		ProjectionExpression:      aws.String("id, priority_timestamp, system_info"),
	})
	if err != nil {
		return nil, err
	}
	var queueRecords []QueueRecord.QRecord         // all Items currently in the Queue
	var selectedRecord *QueueRecord.QRecord        // the next item to process
	var itemsForDLQ []QueueRecord.QRecord          // Items marked for DLQ
	var itemsForReprocessing []QueueRecord.QRecord // Items marked for reprocessing

	if err := attributevalue.UnmarshalListOfMaps(resp.Items, &queueRecords); err != nil {
		ddbc.logger.Err(err).Msg("failed to unmarshal collection")
		return nil, err
	}

	for i := range queueRecords {
		item := &queueRecords[i]
		// Change the AWS Timestamp back into a go time object
		visibilityTime := utils.ParseUnixMilliToTime(item.SystemInfo.VisibilityTimeout)
		if err != nil {
			return nil, err
		}
		// Regardless if the item is visible or not, if it has exceeded the maximum number of retries
		// send it to the DLQ
		if item.SystemInfo.Reprocessed > ddbc.maxRetries {
			// This item has exceeded the maximum number of retries, send it to the DLQ
			ddbc.logger.Debug().Str("op", "peek-dlq").Str("record-id", item.Id).Int64("record-modified-timestamp", item.Priority).Msg("")
			itemsForDLQ = append(itemsForDLQ, *item)
			continue
		}
		// If the current time is after the visibility time, or if the current time is equal to the visibility time
		// and the selectedRecord is nil, then set the selectedRecord to the current item.
		if time.Now().After(visibilityTime) || time.Now().Equal(visibilityTime) {
			// This item is visible, check to see if it needs to be processed
			// If False, this value hasn't been processed yet.
			if !item.SystemInfo.QueueSelected {
				//ddbc.logger.Debug().Str("op", "peek").Str("record-id", item.Id).Str("record-modified-timestamp", item.Priority).Msg("")
				if selectedRecord == nil {
					// This will be the next item to be processed
					ddbc.logger.Debug().Str("op", "peek-selected").Str("record-id", item.Id).Int64("record-modified-timestamp", item.Priority).Msg("")
					selectedRecord = item
					if err := ddbc.markRecordForProcessing(*selectedRecord); err != nil {
						return nil, err
					}
				}
			} else {
				// Visibility timeout has expired, and the Item never finished
				// processing.
				//ddbc.logger.Debug().Str("op", "peek-visibility-timeout").Str("record-id", item.Id).Str("record-modified-timestamp", item.Priority).Msg("visibility timeout exceeded")
				itemsForReprocessing = append(itemsForReprocessing, *item)
			}
		}
	}
	// If we have records that are processing and exceeded their visibility timeout
	// restore the records back into the queue
	if len(itemsForReprocessing) > 0 {
		if err := ddbc.restoreRecords(itemsForReprocessing); err != nil {
			return nil, err
		}
	}
	// If we have records that have exceeded the maximum number of retries
	// send them to the DLQ (This removes them from the queue)
	if len(itemsForDLQ) > 0 {
		if err := ddbc.dlqRecords(itemsForDLQ); err != nil {
			return nil, err
		}
	}

	return selectedRecord, nil
}

func (ddbc *DDBConnection) markRecordForProcessing(record QueueRecord.QRecord) error {
	timestamp := utils.GetCurrentTimeInMilliseconds()
	target, err := ddbc.getRecord(record.Id)
	if err != nil {
		ddbc.logger.Err(err).Msg("failed to fetch sentinel record")
		return err
	}
	// construct the visibility timeout
	visibilityTimeout := utils.ConvertTimeToUnixMilli(time.Now().Add(30 * time.Second))

	upd := expression.
		Set(expression.Name("system_info.queue_selected"), expression.Value(true)).
		Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(timestamp)).
		Add(expression.Name("system_info.version"), expression.Value(aws.Uint(1))).
		Set(expression.Name("system_info.status"), expression.Value(aws.String(QueueRecord.QStatusToString[QueueRecord.Processing]))).
		Set(expression.Name("system_info.queue_peek_timestamp"), expression.Value(timestamp)).
		Set(expression.Name("system_info.visibility_timeout_timestamp"), expression.Value(visibilityTimeout))
	cond := expression.Equal(
		expression.Name("system_info.version"),
		expression.Value(aws.Uint(target.SystemInfo.Version)),
	)
	expr, err := expression.NewBuilder().WithUpdate(upd).WithCondition(cond).Build()
	if err != nil {
		ddbc.logger.Err(err).Msg("failed to build processing expr")
		return err
	}
	_, err = ddbc.dbClient.UpdateItem(context.TODO(), &dynamodb.UpdateItemInput{
		Key:                       QueueRecord.IdToKeyExpr(record.Id),
		TableName:                 aws.String(ddbc.tableName),
		ExpressionAttributeNames:  expr.Names(),
		ExpressionAttributeValues: expr.Values(),
		UpdateExpression:          expr.Update(),
		ConditionExpression:       expr.Condition(),
		ReturnValues:              types.ReturnValueAllNew,
	})
	if err != nil {
		ddbc.logger.Err(err).Str("id", record.Id).
			Uint("p-ver", record.SystemInfo.Version).
			Uint("o-version", target.SystemInfo.Version).
			Msg("failed to update processing record")
		return err
	}
	return nil
}

func (ddbc *DDBConnection) restoreRecords(records []QueueRecord.QRecord) error {
	var updateExpressions []types.TransactWriteItem
	for i := range records {
		item := records[i]
		timestamp := utils.GetCurrentTimeInMilliseconds()
		upd := expression.
			Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
			Set(expression.Name("system_info.visibility_timeout_timestamp"), expression.Value(timestamp)).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(timestamp)).
			Add(expression.Name("system_info.version"), expression.Value(aws.Uint(1))).
			Add(expression.Name("system_info.reprocessed_count"), expression.Value(1)).
			Set(expression.Name("system_info.status"), expression.Value(aws.String(QueueRecord.QStatusToString[QueueRecord.Ready])))
		cond := expression.Equal(
			expression.Name("system_info.version"),
			expression.Value(aws.Uint(item.SystemInfo.Version)),
		)
		expr, err := expression.NewBuilder().WithCondition(cond).WithUpdate(upd).Build()
		if err != nil {
			ddbc.logger.Err(err).Str("record-id", item.Id).Msg("failed to build ddb expression")
		}
		trans := &types.Update{
			Key:                                 QueueRecord.IdToKeyExpr(item.Id),
			TableName:                           aws.String(ddbc.tableName),
			UpdateExpression:                    expr.Update(),
			ConditionExpression:                 expr.Condition(),
			ExpressionAttributeNames:            expr.Names(),
			ExpressionAttributeValues:           expr.Values(),
			ReturnValuesOnConditionCheckFailure: "ALL_OLD",
		}
		updateExpressions = append(updateExpressions, types.TransactWriteItem{Update: trans})
	}
	_, err := ddbc.dbClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems:      updateExpressions,
		ClientRequestToken: aws.String(uuid.New().String()),
	})
	ddbc.logger.Debug().Msg("records visibility-timeout refreshed..")
	if err != nil {
		return err
	}
	return nil
}

func (ddbc *DDBConnection) dlqRecords(records []QueueRecord.QRecord) error {
	var updateExpressions []types.TransactWriteItem
	for i := range records {
		item := records[i]
		timestamp := utils.GetCurrentTimeInMilliseconds()
		upd := expression.
			Set(expression.Name("system_info.queue_selected"), expression.Value(false)).
			Set(expression.Name("system_info.visibility_timeout_timestamp"), expression.Value(timestamp)).
			Set(expression.Name("system_info.last_updated_timestamp"), expression.Value(timestamp)).
			Add(expression.Name("system_info.version"), expression.Value(aws.Int64(1))).
			Set(expression.Name("system_info.queued"), expression.Value(-1)).
			Set(expression.Name("system_info.status"), expression.Value(aws.String(QueueRecord.QStatusToString[QueueRecord.Dlq]))).
			Remove(expression.Name("queued"))
		cond := expression.Equal(
			expression.Name("system_info.version"),
			expression.Value(aws.Uint(item.SystemInfo.Version)),
		)
		expr, err := expression.NewBuilder().WithCondition(cond).WithUpdate(upd).Build()
		if err != nil {
			ddbc.logger.Err(err).Str("record-id", item.Id).Msg("failed to build ddb expression")
		}
		trans := &types.Update{
			Key:                                 QueueRecord.IdToKeyExpr(item.Id),
			TableName:                           aws.String(ddbc.tableName),
			UpdateExpression:                    expr.Update(),
			ConditionExpression:                 expr.Condition(),
			ExpressionAttributeNames:            expr.Names(),
			ExpressionAttributeValues:           expr.Values(),
			ReturnValuesOnConditionCheckFailure: "ALL_OLD",
		}
		updateExpressions = append(updateExpressions, types.TransactWriteItem{Update: trans})
	}
	_, err := ddbc.dbClient.TransactWriteItems(context.TODO(), &dynamodb.TransactWriteItemsInput{
		TransactItems:      updateExpressions,
		ClientRequestToken: aws.String(uuid.New().String()),
	})
	ddbc.logger.Debug().Msg("Records sent to DLQ..")
	if err != nil {
		return err
	}
	return nil
}

func (ddbc *DDBConnection) getRecord(id string) (*QueueRecord.QRecord, error) {
	resp, err := ddbc.dbClient.GetItem(context.TODO(), &dynamodb.GetItemInput{
		Key:            QueueRecord.IdToKeyExpr(id),
		TableName:      aws.String(ddbc.tableName),
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
