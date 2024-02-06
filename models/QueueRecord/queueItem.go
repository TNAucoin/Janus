package QueueRecord

import (
	"fmt"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/google/uuid"
	"github.com/tnaucoin/Janus/utils"
)

type QStatus int

const (
	Pending QStatus = iota
	Ready
	Processing
	Done
	Dlq
)

var QStatusToString = map[QStatus]string{
	Pending:    "PENDING",
	Ready:      "READY",
	Processing: "PROCESSING",
	Done:       "DONE",
	Dlq:        "DLQ",
}

type QRecord struct {
	Id         string       `dynamodbav:"id" json:"id"`
	Priority   int64        `dynamodbav:"priority_timestamp" json:"priority"`
	SystemInfo *QSystemInfo `dynamodbav:"system_info" json:"system_info"`
}
type QSystemInfo struct {
	Created           int64  `dynamodbav:"created_timestamp" json:"created"`
	Id                string `dynamodbav:"id" json:"id"`
	PriorityOffset    int64  `dynamodbav:"priority_offset" json:"priority_offset"`
	LastUpdated       int64  `dynamodbav:"last_updated_timestamp" json:"last_updated"`
	QueueSelected     bool   `dynamodbav:"queue_selected" json:"queue_selected"`
	Queued            int    `dynamodbav:"queued" json:"queued"`
	Reprocessed       int    `dynamodbav:"reprocessed_count" json:"reprocessed"`
	Status            string `dynamodbav:"status" json:"status"`
	Version           uint   `dynamodbav:"version" json:"version"`
	VisibilityTimeout int64  `dynamodbav:"visibility_timeout_timestamp" json:"visibility_timeout"`
}

// IdToKeyExpr takes an ID string and returns a map of key expression for DynamoDB.
// The returned map has a single entry with the key "id" and the value as the given ID string converted to an AttributeValueMemberS.
func IdToKeyExpr(id string) map[string]types.AttributeValue {
	return map[string]types.AttributeValue{
		"id": &types.AttributeValueMemberS{Value: id},
	}
}

// NewQRecord creates a new QRecord object with the given ID.
// It initializes the Priority field with the current time in the "2006-01-02T15:04:05.000Z07:00" format.
// It also creates a new QSystemInfo object using the provided ID and current time,
// and assigns it to the SystemInfo field of the QRecord.
// The returned QRecord object contains the ID, Priority, and SystemInfo fields populated.
func NewQRecord() *QRecord {
	currentTime := utils.GetCurrentTimeInMilliseconds()
	id := uuid.New().String()
	info := newSystemInfo(id, currentTime)
	return &QRecord{
		Id:         id,
		Priority:   currentTime,
		SystemInfo: info,
	}
}

// newSystemInfo takes an ID and current time as strings and returns a pointer to a QSystemInfo struct.
// It initializes the QSystemInfo struct with the provided ID and current time, and sets the other fields to their default values.
// The Created field is set to the current time, the Priority field is set to the current time,
// the QueueSelected field is set to false, the Queued field is set to 0, the Status field is set to "PENDING",
// and the Version field is set to 1.
func newSystemInfo(id string, currentTime int64) *QSystemInfo {
	return &QSystemInfo{
		Created:           currentTime,
		Id:                id,
		LastUpdated:       currentTime,
		QueueSelected:     false,
		Queued:            0,
		Status:            "PENDING",
		Reprocessed:       0,
		Version:           1,
		VisibilityTimeout: currentTime,
	}
}

func (qa *QSystemInfo) String() string {
	return fmt.Sprintf("QSystemInfo: \nCreated: %s\nId: %s\nLastUpdated: %s\nQueueSelected: %t\nQueued: %d\nStatus: %s\nVersion: %d\n",
		qa.Created, qa.Id, qa.LastUpdated, qa.QueueSelected, qa.Queued, qa.Status, qa.Version)
}

func (qr *QRecord) String() string {
	return fmt.Sprintf("QRecord: \nId: %s\nLastUpdated: %s\n%s\n", qr.Id, qr.Priority, qr.SystemInfo)
}
