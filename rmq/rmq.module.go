package rmq

import "github.com/0x626f/gioc"

var Module = gioc.NewModule("RmqModule").
	Provide(
		gioc.FactoryProvider(
			ServiceToken,
			gioc.NewFactory(
				ServiceInjections,
				gioc.Singleton,
				NewRmqService,
			),
			true,
		),
		gioc.FactoryProvider(
			ConsumerServiceToken,
			gioc.NewFactory(
				ConsumerServiceInjections,
				gioc.Singleton,
				NewConsumerService,
			),
			true,
		),
		gioc.FactoryProvider(
			ProducerServiceToken,
			gioc.NewFactory(
				ProducerServiceInjections,
				gioc.Prototype,
				NewProducerService,
			),
			true,
		),
	)
