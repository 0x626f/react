package react

import (
	"context"

	"github.com/0x626f/gioc"
)

var BaseServiceInjections = []gioc.Token{
	ApplicationContextServiceToken,
	LoggerToken,
}

type BaseService struct {
	Ctx  context.Context
	Stop context.CancelFunc

	ApplicationService *ApplicationService
	Logger             ILogger
}

func (service *BaseService) Bootstrap(_ gioc.Token, injections gioc.Injections) {
	injections.Require(BaseServiceInjections...)

	service.ApplicationService = gioc.MustResolve[*ApplicationService](ApplicationContextServiceToken, injections)
	service.Logger = gioc.MustResolve[ILogger](LoggerToken, injections)

	service.Ctx, service.Stop = service.ApplicationService.DeriveContext()
}

type BaseConfigurableService[T any] struct {
	BaseService
	Config T
}

func (service *BaseConfigurableService[T]) Bootstrap(impl gioc.Token, config gioc.Token, injections gioc.Injections) {
	service.BaseService.Bootstrap(impl, injections)
	service.Config = gioc.MustResolve[T](config, injections)
}

func InjectFromBase(tokens ...gioc.Token) []gioc.Token {
	return append(tokens, BaseServiceInjections...)
}
