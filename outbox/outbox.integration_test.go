package outbox

import (
	"os"
	"testing"
)

func requireOutboxIntegrationURL(t testing.TB, variable string, service string) string {
	t.Helper()
	if value := os.Getenv(variable); value != "" {
		return value
	}
	if os.Getenv("REACT_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s not set while integration tests are required", variable)
	}
	t.Skipf("%s not set; skipping %s integration test", variable, service)
	return ""
}
