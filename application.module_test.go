package react

import (
	"context"
	"reflect"
	"testing"
)

func TestApplicationPreShutdownHooksRunBeforeResourceHooks(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	service := &ApplicationService{ctx: ctx, cancel: cancel, shutdownHooksEnabled: true}
	var order []string
	service.AddHook(func() { order = append(order, "resource-one") })
	service.AddPreShutdownHook(func() { order = append(order, "worker-one") })
	service.AddHook(func() { order = append(order, "resource-two") })
	service.AddPreShutdownHook(func() { order = append(order, "worker-two") })
	service.Shutdown()
	want := []string{"worker-one", "worker-two", "resource-one", "resource-two"}
	if !reflect.DeepEqual(order, want) {
		t.Fatalf("shutdown order = %v, want %v", order, want)
	}
	if ctx.Err() == nil {
		t.Fatal("Shutdown did not cancel the application context")
	}
}
