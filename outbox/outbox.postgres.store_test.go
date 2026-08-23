package outbox_test

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/0x626f/author"
	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react"
	"github.com/0x626f/react/outbox"

	reactpostgres "github.com/0x626f/react/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

func TestPostgresConfigRejectsUnsafeIdentifiers(t *testing.T) {
	config := outbox.DefaultPostgresConfig()
	config.Table = `records; DROP TABLE users`
	if _, err := outbox.NewPostgresStore(nopDB{}, config); !errors.Is(err, outbox.ErrInvalidArgument) {
		t.Fatalf("NewPostgresStore error = %v, want ErrInvalidArgument", err)
	}
}

func TestPostgresMigrationContainsFocusedIndexes(t *testing.T) {
	store, err := outbox.NewPostgresStore(nopDB{}, outbox.DefaultPostgresConfig())
	if err != nil {
		t.Fatal(err)
	}
	migrations := store.Migrations()
	if len(migrations) != 1 || migrations[0].Version != 1 {
		t.Fatalf("migrations = %#v", migrations)
	}
	for _, fragment := range []string{"state, available_at", "lease_until", "idempotency", "terminal"} {
		if !strings.Contains(migrations[0].SQL, fragment) {
			t.Errorf("migration does not contain %q", fragment)
		}
	}
}

func TestPostgresForFeatureProvidesPostgresStoreService(t *testing.T) {
	dependencies := gioc.NewModule("OutboxPostgresStoreServiceTestDependencies").Provide(
		outbox.ProvidePostgresConfig(outbox.DefaultPostgresConfig()),
		gioc.ValueProvider(
			reactpostgres.DataSourceToken,
			&pgxext.DataSource{Pool: &pgxpool.Pool{}},
			true,
		),
	)
	outboxModule := outbox.ForFeature(outbox.Postgres).Import(dependencies)
	applicationModule := react.ApplicationModuleFor(react.ApplicationConfig{
		Parent:              context.Background(),
		EnableShutDownHooks: true,
	})
	loggerModule := gioc.NewModule("OutboxPostgresStoreServiceTestLogger").Global().Provide(
		react.Logger(author.Config{Level: author.FATAL}),
	)
	container := gioc.NewContainer()
	if err := container.AddModules(applicationModule, loggerModule, outboxModule); err != nil {
		t.Fatalf("AddModules() = %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("Run() = %v", err)
	}
	application, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve ApplicationService = %v", err)
	}
	t.Cleanup(application.Shutdown)
	store, err := gioc.Get[*outbox.PostgresStoreService](container, outbox.OutboxStoreToken, outboxModule)
	if err != nil {
		t.Fatalf("resolve postgres StoreService = %v", err)
	}
	if store == nil || store.PostgresStore == nil {
		t.Fatal("ForFeature returned a nil postgres StoreService")
	}
}

// nopDB is sufficient for constructor and migration rendering tests.
type nopDB struct{}

func (nopDB) Begin(context.Context) (pgx.Tx, error)                           { panic("not used") }
func (nopDB) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) { panic("not used") }
func (nopDB) Query(context.Context, string, ...any) (pgx.Rows, error)         { panic("not used") }
func (nopDB) QueryRow(context.Context, string, ...any) pgx.Row                { panic("not used") }
