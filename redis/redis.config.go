package redis

import "github.com/0x626f/gioc"

var ConfigToken = gioc.NewToken("RedisConfig")

type Config struct {
	URL string `env:"REDIS_URL"`
}
