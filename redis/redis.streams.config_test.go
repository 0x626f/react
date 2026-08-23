package redis

import (
	"reflect"
	"testing"
	"time"

	"github.com/0x626f/gioc"
	"github.com/0x626f/react"
)

func TestStreamsConfigZeroValueUsesDefaults(t *testing.T) {
	got, err := (StreamsConfig{}).normalized()
	if err != nil {
		t.Fatalf("normalize zero config: %v", err)
	}
	want := DefaultStreamsConfig()
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("normalized config = %+v, want %+v", got, want)
	}
}

func TestStreamsConfigRejectsInvalidBounds(t *testing.T) {
	tests := []struct {
		name      string
		configure func(*StreamsConfig)
	}{
		{name: "workers", configure: func(config *StreamsConfig) { config.WorkerCount = -1 }},
		{name: "channel", configure: func(config *StreamsConfig) { config.ChannelSize = -1 }},
		{name: "consumer count", configure: func(config *StreamsConfig) { config.DefaultConsumerCount = 5 }},
		{name: "batch", configure: func(config *StreamsConfig) { config.DefaultBatchSize = maxStreamsBatchSize + 1 }},
		{name: "block", configure: func(config *StreamsConfig) { config.BlockTimeout = time.Nanosecond }},
		{name: "reclaim", configure: func(config *StreamsConfig) { config.ReclaimAfter = time.Second }},
		{name: "deliveries", configure: func(config *StreamsConfig) { config.MaximumDeliveries = -1 }},
		{name: "retry", configure: func(config *StreamsConfig) { config.RetryMaximumDelay = time.Millisecond }},
		{name: "message", configure: func(config *StreamsConfig) { config.MaximumMessageBytes = -1 }},
		{name: "retention", configure: func(config *StreamsConfig) { config.MaximumStreamLength = -1 }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := DefaultStreamsConfig()
			test.configure(&config)
			if _, err := config.normalized(); err == nil {
				t.Fatalf("normalize invalid config = nil error: %+v", config)
			}
		})
	}
}

func TestStreamsConsumerConfigUsesServiceDefaults(t *testing.T) {
	service := DefaultStreamsConfig()
	got, err := (StreamsConsumerConfig{}).normalized(service)
	if err != nil {
		t.Fatalf("normalize consumer config: %v", err)
	}
	if got.ConsumerCount != service.DefaultConsumerCount || got.BatchSize != service.DefaultBatchSize || got.StartFrom != StreamsStartBeginning {
		t.Fatalf("normalized consumer config = %+v", got)
	}
}

func TestStreamsFeatureDeclaresDedicatedDependencies(t *testing.T) {
	if Streams.Name() != "streams" {
		t.Fatalf("feature name = %q, want streams", Streams.Name())
	}
	for _, token := range []gioc.Token{
		ServiceToken,
		StreamsConfigToken,
		react.ApplicationContextServiceToken,
		react.LoggerToken,
	} {
		found := false
		for _, injection := range StreamsServiceInjections {
			if injection == token {
				found = true
				break
			}
		}
		if !found {
			t.Fatalf("StreamsServiceInjections does not contain %q", token)
		}
	}
}
