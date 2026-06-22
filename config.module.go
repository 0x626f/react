package react

import (
	"path/filepath"
	"runtime"

	"github.com/0x626f/gioc"
	"github.com/0x626f/gota/env"
)

type ConfigModuleParams[T any] struct {
	AppConfigToken gioc.Token
	LoadEnvs       []EnvEntry
	Providers      []ConfigProviders[T]
}

type EnvEntry struct {
	Path     string
	Relative bool
}

type ConfigProviders[T any] struct {
	Token    gioc.Token
	Provider func(*T) (any, error)
}

func RegisterConfigModule[T any](params ConfigModuleParams[T]) *gioc.Module {
	module := gioc.NewModule("Config").Global()

	if len(params.LoadEnvs) > 0 {
		for _, location := range params.LoadEnvs {
			path := location.Path
			if location.Relative {
				_, file, _, _ := runtime.Caller(1)
				path = filepath.Join(filepath.Dir(file), path)
			}
			_ = env.Load(path)
		}
	}
	config := new(T)
	if err := env.Unmarshal(config); err != nil {
		panic(err)
	}

	module.Provide(
		gioc.ValueProvider(params.AppConfigToken, config, true),
	)

	if len(params.Providers) > 0 {
		for _, derivation := range params.Providers {
			module.Provide(
				DeriveConfig(params.AppConfigToken, derivation.Token, derivation.Provider),
			)
		}
	}

	return module
}

func DeriveConfig[T any, R any](originToken, targetToken gioc.Token, provider func(T) (R, error)) gioc.IProvider {
	return gioc.FactoryProvider(
		targetToken,
		gioc.Factory[R]{
			Injects: []gioc.Token{originToken},
			Constructor: func(injections gioc.Injections) (R, error) {
				gioc.Require(injections, originToken)
				origin := gioc.MustResolve[T](originToken, injections)
				return provider(origin)
			},
			ValueScope: gioc.Singleton,
		},
		true,
	)
}
