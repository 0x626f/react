package redis

import (
	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

var StreamsServiceToken = gioc.NewToken("StreamsService")

var StreamsServiceInjections = []gioc.Token{
	ServiceToken,
	StreamsConfigToken,
	react.ApplicationContextServiceToken,
	react.LoggerToken,
}

// Streams enables the lifecycle-owned Redis Streams worker hub.
var Streams = Feature{
	name: "streams",
	providers: func() []gioc.IProvider {
		return []gioc.IProvider{streamsServiceProvider()}
	},
}

func streamsServiceProvider() gioc.IProvider {
	return gioc.FactoryProvider(
		StreamsServiceToken,
		gioc.NewFactory(
			StreamsServiceInjections,
			gioc.Singleton,
			NewStreamsService,
		),
		true,
	)
}
