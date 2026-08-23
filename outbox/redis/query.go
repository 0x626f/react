package redis

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/0x626f/react/outbox"
	goredis "github.com/redis/go-redis/v9"
)

func (store *Store) Get(ctx context.Context, id outbox.ID) (outbox.Record, error) {
	if err := store.ensure(ctx); err != nil {
		return outbox.Record{}, err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return outbox.Record{}, err
	}
	raw, err := store.client.HGet(ctx, store.keys.Records(), string(id)).Result()
	if isNil(err) {
		return outbox.Record{}, outbox.ErrNotFound
	}
	if err != nil {
		return outbox.Record{}, err
	}
	return decodeRecord(raw)
}

func (store *Store) Find(ctx context.Context, query outbox.Query) (outbox.Page, error) {
	if err := store.ensure(ctx); err != nil {
		return outbox.Page{}, err
	}
	query, cursor, err := outbox.NormalizeQuery(query, store.config.Limits)
	if err != nil {
		return outbox.Page{}, err
	}
	if len(query.IDs) > 0 {
		return store.findIDs(ctx, query, cursor)
	}
	if query.Sort != outbox.SortCreatedAt || query.AvailableAt.From != nil || query.AvailableAt.To != nil || len(query.MessageTypes) > 0 || query.AggregateType != "" || query.AggregateID != "" || query.OrderingKey != "" || query.IdempotencyKey != "" {
		return outbox.Page{}, outbox.ErrUnsupportedCriteria
	}
	if len(query.Destinations) > 0 && len(query.States) > 0 {
		return outbox.Page{}, outbox.ErrUnsupportedCriteria
	}
	if len(query.Destinations) > 16 {
		return outbox.Page{}, outbox.ErrUnsupportedCriteria
	}

	type indexSpec struct {
		key, prefix string
		destination bool
	}
	indexes := make([]indexSpec, 0, max(1, len(query.States)+len(query.Destinations)))
	if len(query.Destinations) > 0 {
		for _, destination := range query.Destinations {
			if destination == "" {
				return outbox.Page{}, fmt.Errorf("%w: destination", outbox.ErrInvalidArgument)
			}
			indexes = append(indexes, indexSpec{key: store.keys.QueryDestinations(), prefix: base64.RawURLEncoding.EncodeToString([]byte(destination)) + "|", destination: true})
		}
	} else if len(query.States) > 0 {
		seen := make(map[outbox.State]struct{}, len(query.States))
		for _, state := range query.States {
			if _, exists := seen[state]; exists {
				continue
			}
			seen[state] = struct{}{}
			key, keyErr := store.keys.QueryState(state)
			if keyErr != nil {
				return outbox.Page{}, keyErr
			}
			indexes = append(indexes, indexSpec{key: key})
		}
	} else {
		indexes = append(indexes, indexSpec{key: store.keys.QueryAll()})
	}

	members := make([]string, 0, (query.Limit+1)*len(indexes))
	seenMembers := make(map[string]struct{})
	for _, index := range indexes {
		minimum, maximum := lexBounds(index.prefix, query, cursor)
		rangeBy := &goredis.ZRangeBy{Min: minimum, Max: maximum, Offset: 0, Count: int64(query.Limit + 1)}
		var found []string
		if query.Direction == outbox.SortDescending {
			found, err = store.client.ZRevRangeByLex(ctx, index.key, rangeBy).Result()
		} else {
			found, err = store.client.ZRangeByLex(ctx, index.key, rangeBy).Result()
		}
		if err != nil {
			return outbox.Page{}, err
		}
		for _, member := range found {
			stable := member
			if index.destination {
				stable = strings.TrimPrefix(member, index.prefix)
			}
			if _, exists := seenMembers[stable]; !exists {
				seenMembers[stable] = struct{}{}
				members = append(members, stable)
			}
		}
	}
	sort.Strings(members)
	if query.Direction == outbox.SortDescending {
		reverseStrings(members)
	}
	if len(members) > query.Limit+1 {
		members = members[:query.Limit+1]
	}
	ids := make([]string, len(members))
	for index, member := range members {
		id, parseErr := idFromQueryMember(member)
		if parseErr != nil {
			return outbox.Page{}, parseErr
		}
		ids[index] = string(id)
	}
	if len(ids) == 0 {
		return outbox.Page{Records: []outbox.Record{}}, nil
	}
	values, err := store.client.HMGet(ctx, store.keys.Records(), ids...).Result()
	if err != nil {
		return outbox.Page{}, err
	}
	records := make([]outbox.Record, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok {
			return outbox.Page{}, fmt.Errorf("unexpected Redis record type %T", value)
		}
		record, decodeErr := decodeRecord(raw)
		if decodeErr != nil {
			return outbox.Page{}, decodeErr
		}
		if redisMatches(record, query) {
			records = append(records, record)
		}
	}
	return pageFromRecords(records, query)
}

func (store *Store) findIDs(ctx context.Context, query outbox.Query, cursor outbox.Cursor) (outbox.Page, error) {
	ids := make([]string, len(query.IDs))
	for index, id := range query.IDs {
		ids[index] = string(id)
	}
	values, err := store.client.HMGet(ctx, store.keys.Records(), ids...).Result()
	if err != nil {
		return outbox.Page{}, err
	}
	records := make([]outbox.Record, 0, len(values))
	for _, value := range values {
		if value == nil {
			continue
		}
		raw, ok := value.(string)
		if !ok {
			return outbox.Page{}, fmt.Errorf("unexpected Redis record type %T", value)
		}
		record, decodeErr := decodeRecord(raw)
		if decodeErr != nil {
			return outbox.Page{}, decodeErr
		}
		if redisMatches(record, query) && outbox.RecordAfterCursor(record, cursor) {
			records = append(records, record)
		}
	}
	return pageFromRecords(records, query)
}

func pageFromRecords(records []outbox.Record, query outbox.Query) (outbox.Page, error) {
	outbox.SortRecords(records, query.Sort, query.Direction)
	page := outbox.Page{}
	var err error
	if len(records) > query.Limit {
		page.Records = records[:query.Limit]
		page.NextCursor, err = outbox.CursorForRecord(page.Records[len(page.Records)-1], query.Sort, query.Direction)
	} else {
		page.Records = records
	}
	return page, err
}

func lexBounds(prefix string, query outbox.Query, cursor outbox.Cursor) (minimum, maximum string) {
	minimum, maximum = "-", "+"
	if prefix != "" {
		minimum, maximum = "["+prefix, "["+prefix+"~"
	}
	if query.CreatedAt.From != nil {
		minimum = "[" + prefix + queryMember(query.CreatedAt.From.UnixMicro(), "")
	}
	if query.CreatedAt.To != nil {
		maximum = "[" + prefix + queryMember(query.CreatedAt.To.UnixMicro(), "~")
	}
	if cursor.Version != 0 {
		bound := prefix + queryMember(cursor.Micros, cursor.ID)
		if query.Direction == outbox.SortDescending {
			maximum = "(" + bound
		} else {
			minimum = "(" + bound
		}
	}
	return minimum, maximum
}

func reverseStrings(values []string) {
	for left, right := 0, len(values)-1; left < right; left, right = left+1, right-1 {
		values[left], values[right] = values[right], values[left]
	}
}

func redisMatches(record outbox.Record, query outbox.Query) bool {
	if len(query.States) > 0 && !stateIn(query.States, record.State) {
		return false
	}
	if len(query.Destinations) > 0 && !stringIn(query.Destinations, record.Destination) {
		return false
	}
	if len(query.MessageTypes) > 0 && !stringIn(query.MessageTypes, record.MessageType) {
		return false
	}
	if query.AggregateType != "" && query.AggregateType != record.AggregateType {
		return false
	}
	if query.AggregateID != "" && query.AggregateID != record.AggregateID {
		return false
	}
	if query.OrderingKey != "" && query.OrderingKey != record.OrderingKey {
		return false
	}
	if query.IdempotencyKey != "" && query.IdempotencyKey != record.IdempotencyKey {
		return false
	}
	if !redisTimeInRange(record.CreatedAt, query.CreatedAt) || !redisTimeInRange(record.AvailableAt, query.AvailableAt) {
		return false
	}
	return true
}
func stateIn(values []outbox.State, value outbox.State) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func stringIn(values []string, value string) bool {
	for _, candidate := range values {
		if candidate == value {
			return true
		}
	}
	return false
}
func redisTimeInRange(value time.Time, valueRange outbox.TimeRange) bool {
	return (valueRange.From == nil || !value.Before(*valueRange.From)) && (valueRange.To == nil || !value.After(*valueRange.To))
}

func (store *Store) Cancel(ctx context.Context, id outbox.ID, reason string) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	reason = outbox.BoundFailure(outbox.Failure{Message: reason}, store.config.Limits).Message
	code, err := store.runMutation(ctx, cancelScript, id, reason)
	if err != nil {
		return err
	}
	return mapMutationCode(code)
}

func (store *Store) Reschedule(ctx context.Context, id outbox.ID, availableAt time.Time) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	availableAt = outbox.CanonicalTime(availableAt)
	if err := outbox.ValidateTimestamp("available_at", availableAt); err != nil {
		return err
	}
	code, err := store.runMutation(ctx, rescheduleScript, id, availableAt.UnixMicro())
	if err != nil {
		return err
	}
	return mapMutationCode(code)
}

func (store *Store) Requeue(ctx context.Context, id outbox.ID, options outbox.RequeueOptions) error {
	if err := store.ensure(ctx); err != nil {
		return err
	}
	if err := outbox.ValidateID(id, store.config.Limits); err != nil {
		return err
	}
	if options.MaxAttempts < 0 || options.MaxAttempts > store.config.Limits.MaxAttempts {
		return fmt.Errorf("%w: max attempts", outbox.ErrInvalidArgument)
	}
	available := int64(0)
	if !options.AvailableAt.IsZero() {
		normalized := outbox.CanonicalTime(options.AvailableAt)
		if err := outbox.ValidateTimestamp("available_at", normalized); err != nil {
			return err
		}
		available = normalized.UnixMicro()
	}
	reset := 0
	if options.ResetAttempts {
		reset = 1
	}
	code, err := store.runMutation(ctx, requeueScript, id, available, reset, options.MaxAttempts)
	if err != nil {
		return err
	}
	if code == -5 {
		return fmt.Errorf("%w: requeue requires ResetAttempts or MaxAttempts greater than preserved attempts", outbox.ErrInvalidArgument)
	}
	return mapMutationCode(code)
}

func (store *Store) Purge(ctx context.Context, request outbox.PurgeRequest) (int, error) {
	if err := store.ensure(ctx); err != nil {
		return 0, err
	}
	request, err := outbox.NormalizePurgeRequest(request, store.config.Limits)
	if err != nil {
		return 0, err
	}
	stateStrings := make([]string, len(request.States))
	for index, state := range request.States {
		stateStrings[index] = string(state)
	}
	encoded, _ := json.Marshal(stateStrings)
	code, err := store.runMutation(ctx, purgeScript, string(encoded), request.Before.UnixMicro(), request.Limit)
	if err != nil {
		return 0, err
	}
	return int(code), nil
}

func (store *Store) Backlog(ctx context.Context) (outbox.Backlog, error) {
	if err := store.ensure(ctx); err != nil {
		return outbox.Backlog{}, err
	}
	now, err := redisTime(ctx, store.client)
	if err != nil {
		return outbox.Backlog{}, err
	}
	var pending, leased, dead *goredis.IntCmd
	_, err = store.client.Pipelined(ctx, func(pipe goredis.Pipeliner) error {
		pending = pipe.ZCard(ctx, store.keys.Pending())
		leased = pipe.ZCard(ctx, store.keys.Leased())
		dead = pipe.ZCard(ctx, store.keys.Dead())
		return nil
	})
	if err != nil {
		return outbox.Backlog{}, err
	}
	backlog := outbox.Backlog{Pending: pending.Val(), Leased: leased.Val(), Dead: dead.Val()}
	oldest, err := store.client.ZRangeByScoreWithScores(ctx, store.keys.Pending(), &goredis.ZRangeBy{Min: "-inf", Max: fmt.Sprint(now.UnixMicro()), Offset: 0, Count: 1}).Result()
	if err != nil && !errors.Is(err, goredis.Nil) {
		return outbox.Backlog{}, err
	}
	if len(oldest) == 1 {
		value := time.UnixMicro(int64(oldest[0].Score)).UTC()
		backlog.OldestDueAt = &value
	}
	return backlog, nil
}

var _ outbox.IStore = (*Store)(nil)
var _ outbox.IBacklogReader = (*Store)(nil)
