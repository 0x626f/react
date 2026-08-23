package outbox

import (
	"context"
	"encoding/json"
	"fmt"
)

func (store *RedisStore) Append(ctx context.Context, records ...NewRecord) ([]Record, error) {
	return store.AppendBatch(ctx, AppendRequest{Records: records, DuplicateMode: store.config.DuplicateMode})
}

func (store *RedisStore) AppendBatch(ctx context.Context, request AppendRequest) ([]Record, error) {
	wires, encoded, err := store.prepareAppend(ctx, request)
	if err != nil {
		return nil, err
	}
	if len(wires) == 0 {
		return []Record{}, nil
	}
	value, err := store.runScript(ctx, appendScript, store.keys.ScriptKeys(), int(request.DuplicateMode), encoded)
	if err != nil {
		return nil, err
	}
	result, err := decodeAppendResult(value, len(wires))
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (store *RedisStore) prepareAppend(ctx context.Context, request AppendRequest) ([]wireRecord, string, error) {
	if err := store.ensure(ctx); err != nil {
		return nil, "", err
	}
	if !request.DuplicateMode.Valid() {
		return nil, "", fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	if len(request.Records) == 0 {
		return []wireRecord{}, "", nil
	}
	if len(request.Records) > store.config.Limits.MaxBatchSize {
		return nil, "", fmt.Errorf("%w: append batch exceeds %d", ErrInvalidArgument, store.config.Limits.MaxBatchSize)
	}
	now, err := redisTime(ctx, store.client)
	if err != nil {
		return nil, "", err
	}
	wires := make([]wireRecord, len(request.Records))
	for index, input := range request.Records {
		if input.ID == "" {
			input.ID, err = generateID()
			if err != nil {
				return nil, "", err
			}
		}
		record, prepareErr := PrepareRecord(input, now, store.config.DefaultMaxAttempts, store.config.Limits)
		if prepareErr != nil {
			return nil, "", prepareErr
		}
		wires[index] = wireFromRecord(record)
	}
	encoded, err := json.Marshal(wires)
	if err != nil {
		return nil, "", err
	}
	if len(encoded) > store.config.MaxAppendEncodedBytes {
		return nil, "", fmt.Errorf("%w: encoded append batch exceeds %d bytes", ErrInvalidArgument, store.config.MaxAppendEncodedBytes)
	}
	return wires, string(encoded), nil
}

func decodeAppendResult(value any, expected int) ([]Record, error) {
	values, err := resultArray(value)
	if err != nil || len(values) == 0 {
		return nil, fmt.Errorf("decode Redis append result: %w", err)
	}
	code, err := resultCode(values[0])
	if err != nil {
		return nil, err
	}
	switch code {
	case -2:
		return nil, ErrDuplicateID
	case -3:
		return nil, ErrConflict
	case 0:
	default:
		return nil, fmt.Errorf("unexpected Redis append result code %d", code)
	}
	if len(values)-1 != expected {
		return nil, fmt.Errorf("Redis append returned %d records, want %d", len(values)-1, expected)
	}
	result := make([]Record, expected)
	for index, raw := range values[1:] {
		text, ok := raw.(string)
		if !ok {
			return nil, fmt.Errorf("unexpected Redis append record type %T", raw)
		}
		result[index], err = decodeRecord(text)
		if err != nil {
			return nil, err
		}
	}
	return result, nil
}
