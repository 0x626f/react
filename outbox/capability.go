package outbox

import "github.com/0x626f/gioc"

// ServiceCapabilityProviders exposes the service through narrow named
// capabilities. Ordinary producers should inject IAppender; administrative
// consumers can request only the stronger interface they need.
func ServiceCapabilityProviders(tokens Tokens) []gioc.IProvider {
	provider := func(token gioc.Token, construct func(gioc.Injections) any) gioc.IProvider {
		return gioc.FactoryProvider(
			token,
			gioc.NewFactory(
				[]gioc.Token{tokens.Service},
				gioc.Singleton,
				func(injections gioc.Injections) (any, error) {
					return construct(injections), nil
				},
			),
			true,
		)
	}
	return []gioc.IProvider{
		provider(tokens.Appender, func(injections gioc.Injections) any {
			return gioc.MustResolve[IAppender](tokens.Service, injections)
		}),
		provider(tokens.DeliveryStore, func(injections gioc.Injections) any {
			return gioc.MustResolve[IDeliveryStore](tokens.Service, injections)
		}),
		provider(tokens.Reader, func(injections gioc.Injections) any {
			return gioc.MustResolve[IReader](tokens.Service, injections)
		}),
		provider(tokens.MaintenanceStore, func(injections gioc.Injections) any {
			return gioc.MustResolve[IMaintenanceStore](tokens.Service, injections)
		}),
	}
}
