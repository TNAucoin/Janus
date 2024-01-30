package config

import (
	"github.com/joeshaw/envdecode"
	"log"
)

type Conf struct {
	App   ConfApp
	DB    ConfDB
	Cache ConfCache
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

type ConfCache struct {
	Host     string `env:"CACHE_HOST,required"`
	Port     int    `env:"CACHE_PORT,required"`
	Password string `env:"CACHE_PASSWORD,required"`
}

func New() *Conf {
	var c Conf
	if err := envdecode.StrictDecode(&c); err != nil {
		log.Fatalf("failed to decode: %v", err)
	}
	return &c
}
