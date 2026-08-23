package redis

import "github.com/0x626f/gioc"

var ConfigToken = gioc.NewToken("RedisConfig")

func ProvideConfig(config *Config) gioc.IProvider {
	return gioc.ValueProvider(ConfigToken, config, true)
}

type Config struct {
	URL string `env:"REDIS_URL"`
}
