package utils

import "time"

var (
	timeLayout = "2006-01-02T15:04:05.000Z07:00"
)

func GetCurrentTimeInMilliseconds() int64 {
	return time.Now().UnixMilli()
}

func GetTimeInMillisecondsWithOffset(offset int64) int64 {
	return time.Now().UnixMilli() - offset
}

func ParseUnixMilliToTime(unixTime int64) time.Time {
	seconds := unixTime / int64(1000)     // seconds
	remainingMS := unixTime % int64(1000) // remaining ms
	return time.Unix(seconds, remainingMS*int64(time.Millisecond))
}

func ConvertTimeToUnixMilli(time time.Time) int64 {
	return time.UnixMilli()
}

func ParseAWSFormattedTime(timestamp string) (time.Time, error) {
	parsedTime, err := time.Parse(timeLayout, timestamp)
	if err != nil {
		return time.Time{}, err
	}
	return parsedTime, nil
}
