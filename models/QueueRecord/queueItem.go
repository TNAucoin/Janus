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

type QPriority int64

const (
	NONE QPriority = iota
	LOW
	MEDIUM
	HIGH
)

var QPriorityToInt = map[QPriority]int64{
	NONE:   0,
	LOW:    10,
	MEDIUM: 100,
	HIGH:   200,
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

// NewQRecord takes a recordPriority of type QPriority and returns a new QRecord pointer.
// It generates a unique ID using uuid.New().String().
// It gets the current time in milliseconds using utils.GetCurrentTimeInMilliseconds().
// It gets the priority offset for the given recordPriority using utils.GetTimeInMillisecondsWithOffset(QPriorityToInt[recordPriority]).
// It creates a new QSystemInfo object using the generated ID, current time, and recordPriority.
// It initializes a new QRecord with the generated ID, priorityTime, and the created QSystemInfo object.
// It returns the pointer to the newly created QRecord.
func NewQRecord(recordPriority QPriority) *QRecord {
	currentTime := utils.GetCurrentTimeInMilliseconds()
	// Applies the priority offset to the current time
	// This will give this record a higher/lower priority in queue
	priorityTime := utils.GetTimeInMillisecondsWithOffset(QPriorityToInt[recordPriority])
	id := uuid.New().String()
	info := newSystemInfo(id, currentTime, recordPriority)
	return &QRecord{
		Id:         id,
		Priority:   priorityTime,
		SystemInfo: info,
	}
}

// newSystemInfo takes an ID and current time as strings and returns a pointer to a QSystemInfo struct.
// It initializes the QSystemInfo struct with the provided ID and current time, and sets the other fields to their default values.
// The Created field is set to the current time, the Priority field is set to the current time,
// the QueueSelected field is set to false, the Queued field is set to 0, the Status field is set to "PENDING",
// and the Version field is set to 1.
func newSystemInfo(id string, currentTime int64, recordPriority QPriority) *QSystemInfo {
	return &QSystemInfo{
		Created:           currentTime,
		Id:                id,
		LastUpdated:       currentTime,
		QueueSelected:     false,
		Queued:            0,
		Status:            "PENDING",
		PriorityOffset:    QPriorityToInt[recordPriority],
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
