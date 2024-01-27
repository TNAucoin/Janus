package QueueRecord

import (
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"time"
)

type QRecord struct {
	Id          string       `dynamodbav:"id"`
	LastUpdated string       `dynamodbav:"last_updated_timestamp"`
	SystemInfo  *QSystemInfo `dynamodbav:"system_info"`
}
type QSystemInfo struct {
	Created       string `dynamodbav:"created_timestamp"`
	Id            string `dynamodbav:"id"`
	LastUpdated   string `dynamodbav:"last_updated_timestamp"`
	QueueSelected bool   `dynamodbav:"queue_selected"`
	Queued        int64  `dynamodbav:"queued"`
	Status        string `dynamodbav:"status"`
	Version       int64  `dynamodbav:"version"`
}

// IdToKeyExpr takes an ID string and returns a map of key expression for DynamoDB.
// The returned map has a single entry with the key "id" and the value as the given ID string converted to an AttributeValueMemberS.
func IdToKeyExpr(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: id},
	}
}

// NewQRecord creates a new QRecord object with the given ID.
// It initializes the LastUpdated field with the current time in the "2006-01-02T15:04:05.000Z07:00" format.
// It also creates a new QSystemInfo object using the provided ID and current time,
// and assigns it to the SystemInfo field of the QRecord.
// The returned QRecord object contains the ID, LastUpdated, and SystemInfo fields populated.
func NewQRecord(id string) *QRecord {
	currentTime := time.Now().Format("2006-01-02T15:04:05.000Z07:00")
	info := newSystemInfo(id, currentTime)
	return &QRecord{
		Id:          id,
		LastUpdated: currentTime,
		SystemInfo:  info,
	}
}

// newSystemInfo takes an ID and current time as strings and returns a pointer to a QSystemInfo struct.
// It initializes the QSystemInfo struct with the provided ID and current time, and sets the other fields to their default values.
// The Created field is set to the current time, the LastUpdated field is set to the current time,
// the QueueSelected field is set to false, the Queued field is set to 0, the Status field is set to "PENDING",
// and the Version field is set to 1.
func newSystemInfo(id, currentTime string) *QSystemInfo {
	return &QSystemInfo{
		Created:       currentTime,
		Id:            id,
		LastUpdated:   currentTime,
		QueueSelected: false,
		Queued:        0,
		Status:        "PENDING",
		Version:       1,
	}
}
