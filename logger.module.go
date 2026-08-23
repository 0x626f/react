package react

import (
	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
)

const LoggerToken = "Logger"
const LoggerConfigToken gioc.Token = "LoggerConfig"

// ILogger is React's alias of the logging contract provided by author.
type ILogger = author.ILogger

type LoggerModuleConfig struct {
	Level author.LogLevel `env:"LOG_LEVEL"`
}

func Logger(config author.Config) gioc.IProvider {
	return gioc.FactoryProvider(
		LoggerToken,
		gioc.Factory[ILogger]{
			ValueScope: gioc.Prototype,
			Constructor: func(injections gioc.Injections) (ILogger, error) {
				return author.New(config), nil
			},
		},
		true,
	)
}

func LoggerModule() *gioc.Module {
	module := gioc.NewModule("Logger").Global()

	return module.
		Provide(
			gioc.FactoryProvider(
				LoggerToken,
				gioc.Factory[ILogger]{
					Injects:    []gioc.Token{LoggerConfigToken},
					ValueScope: gioc.Prototype,
					Constructor: func(injections gioc.Injections) (ILogger, error) {
						gioc.Require(injections, LoggerConfigToken)
						config := gioc.MustResolve[*LoggerModuleConfig](LoggerConfigToken, injections)

						return author.New(author.Config{
							Level:           config.Level,
							Timestamp:       true,
							TimestampFormat: "2006-01-02 15:04:05.000",
						}), nil
					},
				},
				true,
			),
		)

}
