package outbox

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"
)

// SortField selects a portable stable time ordering.
type SortField string

const (
	SortCreatedAt   SortField = "created_at"
	SortAvailableAt SortField = "available_at"
)

// SortDirection selects ascending or descending keyset traversal.
type SortDirection string

const (
	SortAscending  SortDirection = "asc"
	SortDescending SortDirection = "desc"
)

// TimeRange is inclusive at both non-nil bounds.
type TimeRange struct {
	From *time.Time
	To   *time.Time
}

// Query is the deliberately limited, storage-independent operational query model.
type Query struct {
	IDs            []ID
	States         []State
	Destinations   []string
	MessageTypes   []string
	AggregateType  string
	AggregateID    string
	OrderingKey    string
	IdempotencyKey string
	CreatedAt      TimeRange
	AvailableAt    TimeRange
	Sort           SortField
	Direction      SortDirection
	Limit          int
	Cursor         string
}

// Page contains copied records and an opaque continuation cursor.
type Page struct {
	Records    []Record
	NextCursor string
}

// Cursor is exported for third-party adapter implementations; applications
// should treat encoded cursor strings as opaque.
type Cursor struct {
	Version   int           `json:"v"`
	Sort      SortField     `json:"s"`
	Direction SortDirection `json:"d"`
	Micros    int64         `json:"t"`
	ID        ID            `json:"i"`
}

// EncodeCursor creates the opaque base64url continuation value.
func EncodeCursor(cursor Cursor) (string, error) {
	if cursor.Version == 0 {
		cursor.Version = 1
	}
	if cursor.Version != 1 {
		return "", invalid("cursor", "unsupported version")
	}
	encoded, err := json.Marshal(cursor)
	if err != nil {
		return "", fmt.Errorf("encode outbox cursor: %w", err)
	}
	return base64.RawURLEncoding.EncodeToString(encoded), nil
}

// DecodeCursor validates a continuation value for adapter implementations.
func DecodeCursor(value string) (Cursor, error) {
	if value == "" {
		return Cursor{}, nil
	}
	if len(value) > 2048 {
		return Cursor{}, invalid("cursor", "exceeds 2048 bytes")
	}
	encoded, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil {
		return Cursor{}, invalid("cursor", "is not valid base64url")
	}
	var cursor Cursor
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.DisallowUnknownFields()
	if err = decoder.Decode(&cursor); err != nil {
		return Cursor{}, invalid("cursor", "is not valid JSON")
	}
	if err = decoder.Decode(&struct{}{}); err != io.EOF {
		return Cursor{}, invalid("cursor", "contains trailing data")
	}
	if cursor.Version != 1 {
		return Cursor{}, invalid("cursor", "has an unsupported version")
	}
	if cursor.Sort != SortCreatedAt && cursor.Sort != SortAvailableAt {
		return Cursor{}, invalid("cursor", "has an invalid sort")
	}
	if cursor.Direction != SortAscending && cursor.Direction != SortDescending {
		return Cursor{}, invalid("cursor", "has an invalid direction")
	}
	if cursor.ID == "" {
		return Cursor{}, invalid("cursor", "has no stable ID tie-breaker")
	}
	if err = ValidateTimestamp("cursor.timestamp", time.UnixMicro(cursor.Micros).UTC()); err != nil {
		return Cursor{}, err
	}
	return cursor, nil
}

// NormalizeQuery applies defaults and validates portable query bounds.
func NormalizeQuery(query Query, limits Limits) (Query, Cursor, error) {
	limits = limits.withDefaults()
	if query.Sort == "" {
		query.Sort = SortCreatedAt
	}
	if query.Direction == "" {
		query.Direction = SortAscending
	}
	if query.Sort != SortCreatedAt && query.Sort != SortAvailableAt {
		return Query{}, Cursor{}, invalid("sort", "is unsupported")
	}
	if query.Direction != SortAscending && query.Direction != SortDescending {
		return Query{}, Cursor{}, invalid("direction", "is unsupported")
	}
	if query.Limit == 0 {
		query.Limit = min(50, limits.MaxPageSize)
	}
	if query.Limit < 1 || query.Limit > limits.MaxPageSize {
		return Query{}, Cursor{}, invalid("limit", fmt.Sprintf("must be between 1 and %d", limits.MaxPageSize))
	}
	if len(query.IDs) > limits.MaxQueryIDs {
		return Query{}, Cursor{}, invalid("ids", fmt.Sprintf("exceed %d entries", limits.MaxQueryIDs))
	}
	for _, id := range query.IDs {
		if err := ValidateID(id, limits); err != nil {
			return Query{}, Cursor{}, err
		}
	}
	if len(query.States) > limits.MaxQueryValues || len(query.Destinations) > limits.MaxQueryValues || len(query.MessageTypes) > limits.MaxQueryValues {
		return Query{}, Cursor{}, invalid("filters", fmt.Sprintf("each repeated filter must contain at most %d values", limits.MaxQueryValues))
	}
	for _, state := range query.States {
		if !state.Valid() {
			return Query{}, Cursor{}, invalid("states", "contains an invalid state")
		}
	}
	for _, destination := range query.Destinations {
		if err := validateRequiredText("destinations", destination, limits.MaxDestinationBytes); err != nil {
			return Query{}, Cursor{}, err
		}
	}
	for _, messageType := range query.MessageTypes {
		if err := validateRequiredText("message_types", messageType, limits.MaxMessageTypeBytes); err != nil {
			return Query{}, Cursor{}, err
		}
	}
	for _, value := range []struct {
		field   string
		value   string
		maximum int
	}{
		{"aggregate_type", query.AggregateType, limits.MaxAggregateTypeBytes},
		{"aggregate_id", query.AggregateID, limits.MaxAggregateIDBytes},
		{"ordering_key", query.OrderingKey, limits.MaxOrderingKeyBytes},
		{"idempotency_key", query.IdempotencyKey, limits.MaxIdempotencyKeyBytes},
	} {
		if err := validateOptionalText(value.field, value.value, value.maximum); err != nil {
			return Query{}, Cursor{}, err
		}
	}
	normalizeRange := func(name string, value *TimeRange) error {
		if value.From != nil {
			normalized := CanonicalTime(*value.From)
			if err := ValidateTimestamp(name+".from", normalized); err != nil {
				return err
			}
			value.From = &normalized
		}
		if value.To != nil {
			normalized := CanonicalTime(*value.To)
			if err := ValidateTimestamp(name+".to", normalized); err != nil {
				return err
			}
			value.To = &normalized
		}
		if value.From != nil && value.To != nil && value.From.After(*value.To) {
			return invalid(name, "start is after end")
		}
		return nil
	}
	if err := normalizeRange("created_at", &query.CreatedAt); err != nil {
		return Query{}, Cursor{}, err
	}
	if err := normalizeRange("available_at", &query.AvailableAt); err != nil {
		return Query{}, Cursor{}, err
	}
	cursor, err := DecodeCursor(query.Cursor)
	if err != nil {
		return Query{}, Cursor{}, err
	}
	if query.Cursor != "" && (cursor.Sort != query.Sort || cursor.Direction != query.Direction) {
		return Query{}, Cursor{}, invalid("cursor", "does not match query ordering")
	}
	if cursor.Version != 0 {
		if err := ValidateID(cursor.ID, limits); err != nil {
			return Query{}, Cursor{}, err
		}
	}
	return query, cursor, nil
}

// RecordSortTime returns the selected stable sort component.
func RecordSortTime(record Record, field SortField) time.Time {
	if field == SortAvailableAt {
		return record.AvailableAt
	}
	return record.CreatedAt
}

// SortRecords orders records by the complete time-plus-ID tuple.
func SortRecords(records []Record, field SortField, direction SortDirection) {
	sort.Slice(records, func(i, j int) bool {
		left, right := RecordSortTime(records[i], field), RecordSortTime(records[j], field)
		comparison := left.Compare(right)
		if comparison == 0 {
			if direction == SortDescending {
				return records[i].ID > records[j].ID
			}
			return records[i].ID < records[j].ID
		}
		if direction == SortDescending {
			return comparison > 0
		}
		return comparison < 0
	})
}

// RecordAfterCursor reports whether a record belongs after a continuation tuple.
func RecordAfterCursor(record Record, cursor Cursor) bool {
	if cursor.Version == 0 {
		return true
	}
	micros := RecordSortTime(record, cursor.Sort).UnixMicro()
	if cursor.Direction == SortDescending {
		return micros < cursor.Micros || (micros == cursor.Micros && record.ID < cursor.ID)
	}
	return micros > cursor.Micros || (micros == cursor.Micros && record.ID > cursor.ID)
}

// CursorForRecord encodes a record's complete stable sort tuple.
func CursorForRecord(record Record, field SortField, direction SortDirection) (string, error) {
	return EncodeCursor(Cursor{Version: 1, Sort: field, Direction: direction, Micros: RecordSortTime(record, field).UnixMicro(), ID: record.ID})
}
