package react

import (
	"context"
	"testing"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
)

func TestReactIntegrationApplicationConfigAndLogger(t *testing.T) {
	configModule := gioc.NewModule("ReactIntegrationConfig").Global().Provide(
		gioc.ValueProvider(LoggerConfigToken, &LoggerModuleConfig{Level: author.NONE}, true),
	)
	container := gioc.NewContainer()
	if err := container.AddModules(
		configModule,
		ApplicationModuleFor(ApplicationConfig{Parent: context.Background(), EnableShutDownHooks: true}),
		LoggerModule(),
	); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}
	application, err := gioc.Get[*ApplicationService](container, ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	logger, err := gioc.Get[ILogger](container, LoggerToken)
	if err != nil || logger == nil {
		t.Fatalf("resolve logger: %v", err)
	}
	derived, cancel := application.DeriveContext()
	defer cancel()
	application.Shutdown()
	if derived.Err() == nil {
		t.Fatal("application shutdown did not cancel a derived service context")
	}
}
