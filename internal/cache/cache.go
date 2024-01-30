package cache

import (
	"context"
	"errors"
	"fmt"
	"github.com/redis/go-redis/v9"
)

type RedisCache struct {
	Client *redis.Client
}

func New(host, password string, port int) *RedisCache {
	addr := fmt.Sprintf("%s:%d", host, port)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Password: password,
		DB:       0,
	})
	return &RedisCache{
		Client: rdb,
	}
}

func (rc *RedisCache) Enqueue(record, queueName string, score int64) {
	rc.addToSortedSet(record, queueName, score)
	rc.showVisible(record, queueName)
}

func (rc *RedisCache) Dequeue(queueName string) (string, error) {
	result, err := rc.Client.ZRange(context.Background(), queueName, 0, -1).Result()
	if err != nil {
		return "", err
	}
	// TODO: need to do this better, this could get heavy under load
	// Loop through the sorted set, and pick the lowest score that is marked visible
	// make it hidden, and return it (Dequeue)
	for _, v := range result {
		visibility, err := rc.Client.HGet(context.Background(), fmt.Sprintf("%s:visible", queueName), v).Result()
		if err != nil {
			fmt.Printf("Failed to get visibility for item %s: %v\n", v, err)
			continue
		}
		if visibility == "1" {
			// mark the first match as hidden, and return it
			rc.hideVisible(v, queueName)
			return v, nil
		}
	}
	return result[0], nil
}

func (rc *RedisCache) RemoveFromQueue(record, queueName string) {
	rc.deleteRecordFromVisibleQueue(record, queueName)
	rc.deleteFromSortedSet(record, queueName)
}

func (rc *RedisCache) ListQueueElements(queueName string) error {
	val, err := rc.Client.ZRangeWithScores(context.Background(), queueName, 0, -1).Result()
	if err != nil {
		return err
	}
	for _, v := range val {
		memberStr, ok := v.Member.(string)
		if !ok {
			return errors.New("failed to cast Member to string")
		}
		visibility, err := rc.Client.HGet(context.Background(), fmt.Sprintf("%s:visible", queueName), memberStr).Result()
		if err != nil {
			fmt.Printf("Failed to get visibility for item %s: %v\n", v.Member, err)
			continue
		}
		if visibility == "1" {
			fmt.Printf("Visible member: %s with score: %.f\n", v.Member, v.Score)
		}
	}
	return nil
}

func (rc *RedisCache) addToSortedSet(record, queueName string, score int64) {
	var z = redis.Z{
		Score:  float64(score),
		Member: record,
	}
	rc.Client.ZAdd(context.Background(), queueName, z)
}

func (rc *RedisCache) deleteFromSortedSet(record, queueName string) {
	rc.Client.ZRem(context.Background(), queueName, record)
}
func (rc *RedisCache) showVisible(record, queueName string) {
	rc.Client.HSet(context.Background(), fmt.Sprintf("%s:visible", queueName), record, 1)
}
func (rc *RedisCache) hideVisible(record, queueName string) {
	rc.Client.HSet(context.Background(), fmt.Sprintf("%s:visible", queueName), record, 0)
}
func (rc *RedisCache) deleteRecordFromVisibleQueue(record, queueName string) {
	rc.Client.HDel(context.Background(), fmt.Sprintf("%s:visible", queueName), record)
}
