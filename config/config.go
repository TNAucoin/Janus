package config

import (
	"github.com/joeshaw/envdecode"
	"log"
)

type Conf struct {
	App   ConfApp
	DB    ConfDB
	Queue ConfQueue
	MQ    ConfMQ
}

type ConfApp struct {
	Env string `env:"APP_ENV,default=development"`
}

type ConfDB struct {
	Host      string `env:"DB_LOCAL_HOST,default=localhost"`
	Port      int    `env:"DB_LOCAL_PORT,default=8000"`
	TableName string `env:"DB_TABLE_NAME,required"`
	Region    string `env:"DB_REGION,required"`
	IndexName string `env:"DB_TABLE_INDEX_NAME,required"`
}

type ConfQueue struct {
	MaxRetries int `env:"QUEUE_MAX_RETRIES,default=3"`
}

type ConfMQ struct {
	User     string `env:"MQ_USER,required"`
	Password string `env:"MQ_PASS,required"`
	Host     string `env:"MQ_HOST,required"`
	Port     int    `env:"MQ_PORT,required"`
	Protocol string `env:"MQ_PROTOCOL,required"`
	VHost    string `env:"MQ_VHOST,required"`
}

func New() *Conf {
	var c Conf
	if err := envdecode.StrictDecode(&c); err != nil {
		log.Fatalf("failed to decode: %v", err)
	}
	return &c
}
