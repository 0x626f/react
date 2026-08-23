package postgres

import (
	"context"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react"
)

var ConfigToken = gioc.NewToken("PostgresConfig")
var DataSourceToken = gioc.NewToken("PostgresDataSource")

type RepositoryConstructor struct {
	Token       gioc.Token
	Constructor func(*pgxext.DataSource) (any, error)
}

func Repository[T any](token gioc.Token, constructor func(*pgxext.DataSource) *T) RepositoryConstructor {
	return RepositoryConstructor{
		Token: token,
		Constructor: func(ds *pgxext.DataSource) (any, error) {
			return constructor(ds), nil
		},
	}
}

func provideRepositories(constructors []RepositoryConstructor) (repositories []gioc.IProvider) {
	for _, repository := range constructors {
		repository := repository
		repositories = append(repositories, gioc.FactoryProvider(
			repository.Token,
			gioc.NewFactory(
				[]gioc.Token{DataSourceToken},
				gioc.Singleton,
				func(injections gioc.Injections) (any, error) {
					return repository.Constructor(
						gioc.MustResolve[*pgxext.DataSource](DataSourceToken, injections),
					)
				},
			),
			true,
		))
	}
	return
}

var Module = gioc.NewModule("PostgresModule").
	Global().
	Provide(
		gioc.FactoryProvider(
			DataSourceToken,
			gioc.NewFactory(
				[]gioc.Token{react.ApplicationContextToken, ConfigToken},
				gioc.Singleton,
				func(injections gioc.Injections) (ds *pgxext.DataSource, err error) {
					ctx := gioc.MustResolve[context.Context](react.ApplicationContextToken, injections)
					config := gioc.MustResolve[*Config](ConfigToken, injections)

					clientConfig := &pgxext.Config{}
					if clientConfig, err = clientConfig.WithURL(config.URL); err != nil {
						return nil, err
					}

					return pgxext.NewDataSource(ctx, clientConfig)
				},
			),
			true,
		),
	)

func ForRepositories(constructors []RepositoryConstructor) *gioc.Module {
	return Module.
		Provide(
			provideRepositories(constructors)...,
		)
}
