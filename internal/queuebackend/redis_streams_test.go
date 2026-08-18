package queuebackend

import "github.com/redis/go-redis/v9"

func redisMessageForTest(values map[string]any) redis.XMessage {
	return redis.XMessage{ID: "1-0", Values: values}
}
