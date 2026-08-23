package rmq

import (
	"context"
	"errors"
	"testing"
)

func TestWaitReadyContextHonorsCallerCancellation(t *testing.T) {
	serviceCtx, stopService := context.WithCancel(context.Background())
	defer stopService()
	ready := make(chan struct{})
	close(ready)
	service := &Service{ready: ready}
	service.Ctx = serviceCtx

	requestCtx, cancelRequest := context.WithCancel(context.Background())
	cancelRequest()
	if err := service.WaitReadyContext(requestCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReadyContext() = %v, want context.Canceled", err)
	}
}

func TestWaitReadyContextHonorsServiceCancellation(t *testing.T) {
	serviceCtx, stopService := context.WithCancel(context.Background())
	service := &Service{ready: make(chan struct{})}
	service.Ctx = serviceCtx
	stopService()

	if err := service.WaitReadyContext(context.Background()); !errors.Is(err, context.Canceled) {
		t.Fatalf("WaitReadyContext() = %v, want context.Canceled", err)
	}
}

func TestWaitReadyContextRejectsNilContext(t *testing.T) {
	service := &Service{ready: make(chan struct{})}
	service.Ctx = context.Background()
	if err := service.WaitReadyContext(nil); err == nil {
		t.Fatal("WaitReadyContext(nil) succeeded")
	}
}
