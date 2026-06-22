package rmq

import (
	"slices"
	"testing"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

func TestModuleConfigBuildConnectionUrl(t *testing.T) {
	tests := []struct {
		name   string
		config ModuleConfig
		want   string
	}{
		{
			name: "host and port only",
			config: ModuleConfig{
				Host: "localhost",
				Port: 5672,
			},
			want: "amqp://localhost:5672",
		},
		{
			name: "credentials are escaped",
			config: ModuleConfig{
				Host:     "rabbitmq.local",
				Port:     5672,
				User:     "test user",
				Password: "p@ss:word",
			},
			want: "amqp://test%20user:p%40ss%3Aword@rabbitmq.local:5672",
		},
		{
			name: "ipv6 host",
			config: ModuleConfig{
				Host: "::1",
				Port: 5672,
			},
			want: "amqp://[::1]:5672",
		},
		{
			name: "partial credentials are ignored",
			config: ModuleConfig{
				Host: "localhost",
				Port: 5672,
				User: "guest",
			},
			want: "amqp://localhost:5672",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.config.buildConnectionUrl(); got != test.want {
				t.Fatalf("buildConnectionUrl() = %q, want %q", got, test.want)
			}
		})
	}
}

func TestProvideModuleConfig(t *testing.T) {
	config := &ModuleConfig{
		Host:       "localhost",
		Port:       5672,
		RetryCount: 3,
		RetryDelay: time.Second,
	}

	provider := ProvideModuleConfig(config)
	if provider.Token() != ModuleConfigToken {
		t.Fatalf("provider token = %q, want %q", provider.Token(), ModuleConfigToken)
	}
	if provider.Exportable() {
		t.Fatal("module config provider should not be exportable by default")
	}
	if provider.Scope() != gioc.Singleton {
		t.Fatalf("provider scope = %v, want %v", provider.Scope(), gioc.Singleton)
	}

	injection, err := provider.Create(nil)
	if err != nil {
		t.Fatalf("provider.Create() error = %v", err)
	}
	got, err := gioc.Resolve[*ModuleConfig](ModuleConfigToken, gioc.Injections{injection})
	if err != nil {
		t.Fatalf("resolve module config: %v", err)
	}
	if got != config {
		t.Fatal("provider did not return the original config pointer")
	}
}

func TestServiceInjections(t *testing.T) {
	assertInjection(t, ServiceInjections, ModuleConfigToken)
	assertInjection(t, ServiceInjections, react.ApplicationContextServiceToken)
	assertInjection(t, ServiceInjections, react.LoggerToken)

	assertInjection(t, ConsumerServiceInjections, ServiceToken)
	assertInjection(t, ConsumerServiceInjections, react.ApplicationContextServiceToken)
	assertInjection(t, ConsumerServiceInjections, react.LoggerToken)

	assertInjection(t, ProducerServiceInjections, ServiceToken)
	assertInjection(t, ProducerServiceInjections, react.ApplicationContextServiceToken)
	assertInjection(t, ProducerServiceInjections, react.LoggerToken)
}

func assertInjection(t *testing.T, injections []gioc.Token, token gioc.Token) {
	t.Helper()
	if !slices.Contains(injections, token) {
		t.Fatalf("injections %v do not include %q", injections, token)
	}
}
