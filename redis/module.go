package redis

import "github.com/0x626f/gioc"

var ConfigToken = gioc.NewToken("RedisConfig")
var ServiceToken = gioc.NewToken("RedisService")

type Config struct {
	URL string `env:"REDIS_URL"`
}

var Module = gioc.NewModule("RedisModule").
	Global().
	Provide(
		gioc.FactoryProvider(
			ServiceToken,
			gioc.NewFactory(
				ServiceInjections,
				gioc.Singleton,
				NewService,
			),
			true,
		),
	)
