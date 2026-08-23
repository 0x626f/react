package outbox_test

import (
	"testing"
	"time"

	"github.com/0x626f/react/outbox"
)

func TestExponentialBackoffBoundsAndAttempts(t *testing.T) {
	policy, err := outbox.NewExponentialBackoff(outbox.ExponentialBackoffConfig{
		Minimum: time.Second, Maximum: 5 * time.Second, Multiplier: 2,
		Jitter: 0, MaxAttempts: 4,
	})
	if err != nil {
		t.Fatal(err)
	}
	if delay, retry := policy.Next(1, nil); !retry || delay != time.Second {
		t.Fatalf("attempt 1 = %v,%v", delay, retry)
	}
	if delay, retry := policy.Next(3, nil); !retry || delay < time.Second || delay > 5*time.Second {
		t.Fatalf("attempt 3 = %v,%v", delay, retry)
	}
	if _, retry := policy.Next(4, nil); retry {
		t.Fatal("maximum attempt was retried")
	}
}
