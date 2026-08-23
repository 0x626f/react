package redis

import (
	"context"
	"fmt"
	"strings"

	"github.com/0x626f/react/outbox"
)

// CompositionRequest atomically combines a Redis-backed domain mutation with
// an outbox append in one server-side script. DomainKeys are appended after the
// 15 outbox keys and DomainArguments after the two append arguments. Trusted
// Lua snippets address them with DOMAIN_KEY_OFFSET and DOMAIN_ARG_OFFSET.
//
// ValidateLua must perform every domain precondition and type check without
// writing. ApplyLua runs only after both domain and outbox duplicate validation
// have succeeded. It must use only commands whose runtime types were validated;
// Redis does not roll back writes when a later command raises a runtime error.
// Both snippets are deployment code, never untrusted request input.
type CompositionRequest struct {
	Append          outbox.AppendRequest
	DomainKeys      []string
	DomainArguments []any
	ValidateLua     string
	ApplyLua        string
}

// Compose performs one atomic Redis-backed domain mutation and append. It does
// not make mutations in PostgreSQL or another Redis hash slot atomic.
func (store *Store) Compose(ctx context.Context, request CompositionRequest) ([]outbox.Record, error) {
	if len(request.DomainKeys) == 0 {
		return nil, fmt.Errorf("%w: domain keys are required", outbox.ErrInvalidArgument)
	}
	if len(request.DomainKeys) > 32 || len(request.DomainArguments) > 64 {
		return nil, fmt.Errorf("%w: composition work limit", outbox.ErrInvalidArgument)
	}
	if request.ValidateLua == "" || request.ApplyLua == "" || len(request.ValidateLua)+len(request.ApplyLua) > 32*1024 {
		return nil, fmt.Errorf("%w: composition Lua", outbox.ErrInvalidArgument)
	}
	outboxKeys := store.keys.ScriptKeys()
	slot := ClusterSlot(outboxKeys[0])
	for _, key := range request.DomainKeys {
		if key == "" || ClusterSlot(key) != slot || redisHashTag(key) != store.keys.Namespace() {
			return nil, fmt.Errorf("%w: every domain key must use the outbox namespace hash tag", outbox.ErrInvalidArgument)
		}
	}
	wires, encoded, err := store.prepareAppend(ctx, request.Append)
	if err != nil {
		return nil, err
	}
	if len(wires) == 0 {
		return nil, fmt.Errorf("%w: composition append cannot be empty", outbox.ErrInvalidArgument)
	}
	const validationHook = "-- react-outbox:domain-validation-hook"
	const applyHook = "-- react-outbox:domain-apply-hook"
	beforeValidation, remainder, found := strings.Cut(appendScript.source, validationHook)
	if !found {
		return nil, fmt.Errorf("Redis outbox composition validation hook is unavailable")
	}
	betweenHooks, afterApply, found := strings.Cut(remainder, applyHook)
	if !found {
		return nil, fmt.Errorf("Redis outbox composition apply hook is unavailable")
	}
	source := beforeValidation + request.ValidateLua + betweenHooks + request.ApplyLua + afterApply
	keys := append(append([]string(nil), outboxKeys...), request.DomainKeys...)
	arguments := []any{int(request.Append.DuplicateMode), encoded}
	arguments = append(arguments, request.DomainArguments...)
	value, err := store.runScript(ctx, newLuaScript(source), keys, arguments...)
	if err != nil {
		return nil, err
	}
	result, err := decodeAppendResult(value, len(wires))
	return result, err
}
