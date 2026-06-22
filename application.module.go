package react

import (
	"context"
	"os"
	"os/signal"

	"github.com/0x626f/gioc"
)

const (
	ApplicationContextServiceToken = "ApplicationService"
	ApplicationContextToken        = "ApplicationContext"
)

type ShutDownHook func()

type ApplicationConfig struct {
	Parent              context.Context
	Interruptions       []os.Signal
	EnableShutDownHooks bool
}

func ApplicationModuleFor(config ApplicationConfig) *gioc.Module {
	module := gioc.NewModule("ApplicationContext").Global()

	if config.Parent == nil {
		config.Parent = context.Background()
	}

	var ctx context.Context
	var cancel context.CancelFunc

	if len(config.Interruptions) > 0 {
		ctx, cancel = signal.NotifyContext(config.Parent, config.Interruptions...)
	} else {
		ctx, cancel = context.WithCancel(config.Parent)
	}

	service := &ApplicationService{
		ctx:                  ctx,
		cancel:               cancel,
		shutdownHooksEnabled: config.EnableShutDownHooks,
	}

	return module.
		Provide(
			gioc.ValueProvider(ApplicationContextServiceToken, service, true),
			gioc.ValueProvider(ApplicationContextToken, ctx, true),
		)
}

func ApplicationContext(ctx context.Context) gioc.IProvider {
	return gioc.ValueProvider(ApplicationContextToken, ctx, true)
}

type ApplicationService struct {
	ctx    context.Context
	cancel context.CancelFunc

	shutdownHooksEnabled bool
	shutdownHooks        []ShutDownHook
}

func (service *ApplicationService) Await() {
	<-service.ctx.Done()
}

func (service *ApplicationService) Shutdown() {
	if service.shutdownHooksEnabled {
		for _, hook := range service.shutdownHooks {
			hook()
		}
	}
	service.cancel()
}

func (service *ApplicationService) AddHook(hook ShutDownHook) {
	service.shutdownHooks = append(service.shutdownHooks, hook)
}

func (service *ApplicationService) DeriveContext() (context.Context, context.CancelFunc) {
	return context.WithCancel(service.ctx)
}
