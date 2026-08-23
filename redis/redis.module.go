package redis

import (
	"fmt"

	"github.com/0x626f/gioc"
)

var ServiceToken = gioc.NewToken("RedisService")

// Feature is an immutable Redis capability selected by ForFeature. Features
// add only their own providers; the base Redis service is always available.
type Feature struct {
	name      string
	providers func() []gioc.IProvider
}

// Name returns the stable feature name used in the generated module token.
func (feature Feature) Name() string { return feature.name }

// ForFeature creates a fresh Redis module with the requested optional
// capabilities. Select redis.Streams to make StreamsServiceToken
// injectable. Its dedicated config remains the application's responsibility.
func ForFeature(features ...Feature) *gioc.Module {
	moduleName := "RedisModule"
	seen := make(map[string]struct{}, len(features))
	providers := []gioc.IProvider{redisServiceProvider()}
	for _, feature := range features {
		if _, exists := seen[feature.name]; exists {
			continue
		}
		seen[feature.name] = struct{}{}
		moduleName += ":" + feature.name
		if feature.name == "" || feature.providers == nil {
			providers = append(providers, invalidRedisFeatureProvider(feature))
			continue
		}
		providers = append(providers, feature.providers()...)
	}

	return gioc.NewModule(gioc.NewToken(moduleName)).
		Global().
		Provide(providers...)
}

func redisServiceProvider() gioc.IProvider {
	return gioc.FactoryProvider(
		ServiceToken,
		gioc.NewFactory(
			ServiceInjections,
			gioc.Singleton,
			NewService,
		),
		true,
	)
}

func invalidRedisFeatureProvider(feature Feature) gioc.IProvider {
	return gioc.FactoryProvider(
		gioc.NewToken("RedisFeature:invalid:"+feature.name),
		gioc.NewFactory(
			nil,
			gioc.Singleton,
			func(gioc.Injections) (any, error) {
				return nil, fmt.Errorf("redis feature %q is invalid", feature.name)
			},
		),
		true,
	)
}

// Module preserves the base Redis module API for applications that do not use
// an optional feature.
var Module = ForFeature()
