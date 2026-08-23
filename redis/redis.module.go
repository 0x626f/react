package redis

import "github.com/0x626f/gioc"

var ServiceToken = gioc.NewToken("RedisService")

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
