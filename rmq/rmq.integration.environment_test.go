package rmq

import (
	"os"
	"testing"
)

func requireRmqIntegrationURL(t testing.TB) string {
	t.Helper()
	const variable = "RMQ_TEST_URL"
	if value := os.Getenv(variable); value != "" {
		return value
	}
	if os.Getenv("REACT_REQUIRE_INTEGRATION") == "1" {
		t.Fatalf("%s not set while integration tests are required", variable)
	}
	t.Skipf("%s not set; skipping RabbitMQ integration test", variable)
	return ""
}
