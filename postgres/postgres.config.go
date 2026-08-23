package postgres

import "github.com/0x626f/gioc"

type Config struct {
	URL string `env:"POSTGRES_URL"`
}

func ProvideConfig(config *Config) gioc.IProvider {
	return gioc.ValueProvider(ConfigToken, config, true)
}
