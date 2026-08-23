package postgres

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/pgxext"
	"github.com/0x626f/react"
)

func TestPostgresIntegrationModuleAndRepository(t *testing.T) {
	rawURL := requirePostgresIntegrationURL(t)
	type integrationRepository struct{ dataSource *pgxext.DataSource }
	repositoryToken := gioc.NewToken("PostgresIntegrationRepository")
	postgresModule := ForRepositories([]RepositoryConstructor{
		Repository(repositoryToken, func(dataSource *pgxext.DataSource) *integrationRepository {
			return &integrationRepository{dataSource: dataSource}
		}),
	})
	configModule := gioc.NewModule("PostgresIntegrationConfig").Global().Provide(
		ProvideConfig(&Config{URL: rawURL}),
	)
	container := gioc.NewContainer()
	if err := container.AddModules(
		configModule,
		react.ApplicationModuleFor(react.ApplicationConfig{Parent: context.Background(), EnableShutDownHooks: true}),
		postgresModule,
	); err != nil {
		t.Fatalf("add modules: %v", err)
	}
	if err := container.Run(); err != nil {
		t.Fatalf("run container: %v", err)
	}
	application, err := gioc.Get[*react.ApplicationService](container, react.ApplicationContextServiceToken)
	if err != nil {
		t.Fatalf("resolve application service: %v", err)
	}
	dataSource, err := gioc.Get[*pgxext.DataSource](container, DataSourceToken)
	if err != nil {
		t.Fatalf("resolve data source: %v", err)
	}
	t.Cleanup(func() {
		dataSource.Close()
		application.Shutdown()
	})
	repository, err := gioc.Get[*integrationRepository](container, repositoryToken)
	if err != nil {
		t.Fatalf("resolve repository: %v", err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	row, err := repository.dataSource.QueryRow(ctx, `SELECT 42`)
	if err != nil {
		t.Fatalf("query row: %v", err)
	}
	var value int
	if err = row.Scan(&value); err != nil {
		t.Fatalf("scan: %v", err)
	}
	if value != 42 {
		t.Fatalf("query value = %d, want 42", value)
	}
}

func requirePostgresIntegrationURL(t testing.TB) string {
	t.Helper()
	const variable = "POSTGRES_TEST_URL"
	if value := os.Getenv(variable); value != "" {
		return value
	}
	if os.Getenv("REACT_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s not set while integration tests are required", variable)
	}
	t.Skipf("%s not set; skipping PostgreSQL integration test", variable)
	return ""
}
