package utils

import "time"

var (
	timeLayout = "2006-01-02T15:04:05.000Z07:00"
)

func GetCurrentTimeInMilliseconds() int64 {
	return time.Now().UnixMilli()
}

func GetCurrentTimeAWSFormatted() string {
	return time.Now().Format(timeLayout)
}

func ConvertTimeAWSFormatted(t time.Time) string {
	return t.Format(timeLayout)
}

func ParseAWSFormattedTime(timestamp string) (time.Time, error) {
	parsedTime, err := time.Parse(timeLayout, timestamp)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}
