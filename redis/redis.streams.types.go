package redis

import (
	"encoding/json"
	"fmt"
)

// StreamsStart is the initial group position used only when Consume has to
// create the group. Existing groups keep their persisted position.
type StreamsStart string

const (
	StreamsStartBeginning StreamsStart = "0-0"
	StreamsStartLatest    StreamsStart = "$"
)

// StreamsConsumerConfig overrides per-subscription read concurrency and
// batch size. The output channel capacity always comes from StreamsConfig
// so memory use remains bounded at service level.
type StreamsConsumerConfig struct {
	ConsumerCount int
	BatchSize     int64
	StartFrom     StreamsStart
}

// StreamsMessage is one manual-acknowledgement group delivery. Payload is
// the JSON value supplied to Publish. Attempts starts at one and increases when
// an idle pending entry is reclaimed.
type StreamsMessage struct {
	ID       string
	Stream   string
	Group    string
	Key      string
	Attempts int64
	Payload  json.RawMessage

	receipt streamsReceipt
}

type streamsReceipt struct {
	consumer string
	service  *StreamsService
	stream   string
	group    string
	id       string
	attempts int64
}

// Decode unmarshals the message payload into target.
func (message StreamsMessage) Decode(target any) error {
	if target == nil {
		return fmt.Errorf("redis stream decode target is required")
	}
	if err := json.Unmarshal(message.Payload, target); err != nil {
		return fmt.Errorf("decode redis stream message %q: %w", message.ID, err)
	}
	return nil
}
