package outbox

import (
	"bytes"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode/utf8"
)

// MaximumTimestampUnixMicro is the largest portable timestamp. Microseconds
// through this value are exactly representable by the numeric type used by
// Redis Lua and are also supported by the other first-party adapters.
const MaximumTimestampUnixMicro int64 = 1<<53 - 1

// ID is a stable application-visible record identifier.
type ID string

// NewRecord contains immutable message content and its initial delivery policy.
type NewRecord struct {
	ID             ID
	Destination    string
	MessageType    string
	AggregateType  string
	AggregateID    string
	OrderingKey    string
	IdempotencyKey string
	Headers        map[string]string
	Payload        []byte
	AvailableAt    time.Time
	MaxAttempts    int
}

// Record is the copied public snapshot of one persisted outbox record.
type Record struct {
	ID             ID
	Destination    string
	MessageType    string
	AggregateType  string
	AggregateID    string
	OrderingKey    string
	IdempotencyKey string
	Headers        map[string]string
	Payload        []byte

	// ContentDigest is a lowercase SHA-256 digest of immutable message content.
	// It deliberately excludes ID and mutable delivery scheduling fields so an
	// idempotency key can identify the same message submitted with a newly
	// generated ID or after the existing record has been rescheduled.
	ContentDigest string

	State       State
	Attempts    int
	MaxAttempts int
	AvailableAt time.Time

	LeaseOwner string
	LeaseToken string
	LeaseUntil *time.Time

	LastErrorCode    string
	LastErrorMessage string

	CreatedAt   time.Time
	UpdatedAt   time.Time
	DeliveredAt *time.Time
	DeadAt      *time.Time
	CancelledAt *time.Time
	Version     uint64
}

// Clone returns a deep copy suitable for crossing an API boundary.
func (r Record) Clone() Record {
	r.Headers = cloneHeaders(r.Headers)
	r.Payload = bytes.Clone(r.Payload)
	r.LeaseUntil = cloneTime(r.LeaseUntil)
	r.DeliveredAt = cloneTime(r.DeliveredAt)
	r.DeadAt = cloneTime(r.DeadAt)
	r.CancelledAt = cloneTime(r.CancelledAt)
	return r
}

// Clone returns a deep copy of an append request.
func (r NewRecord) Clone() NewRecord {
	r.Headers = cloneHeaders(r.Headers)
	r.Payload = bytes.Clone(r.Payload)
	return r
}

func cloneHeaders(headers map[string]string) map[string]string {
	if headers == nil {
		return nil
	}
	copy := make(map[string]string, len(headers))
	for key, value := range headers {
		copy[key] = value
	}
	return copy
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	copy := CanonicalTime(*value)
	return &copy
}

// CanonicalTime is the timestamp representation shared by all adapters.
// PostgreSQL and Redis both preserve microseconds, and Unix microseconds remain
// exactly representable by Redis Lua numbers for the supported date range.
func CanonicalTime(value time.Time) time.Time {
	if value.IsZero() {
		return value
	}
	return value.UTC().Truncate(time.Microsecond)
}

// Limits bounds data and work accepted at public API boundaries.
type Limits struct {
	MaxIDBytes             int
	MaxDestinationBytes    int
	MaxMessageTypeBytes    int
	MaxAggregateTypeBytes  int
	MaxAggregateIDBytes    int
	MaxOrderingKeyBytes    int
	MaxIdempotencyKeyBytes int
	MaxPayloadBytes        int
	MaxHeaders             int
	MaxHeaderKeyBytes      int
	MaxHeaderValueBytes    int
	MaxHeaderBytes         int
	MaxErrorCodeBytes      int
	MaxErrorMessageBytes   int
	MaxLeaseOwnerBytes     int
	MaxLeaseTokenBytes     int
	MaxBatchSize           int
	MaxClaimBatchSize      int
	MaxPageSize            int
	MaxQueryIDs            int
	MaxQueryValues         int
	MaxPurgeSize           int
	MaxAttempts            int
	MaxWorkerCount         int
	MaxDestinationWorkers  int
}

// DefaultLimits returns the portable first-party resource bounds.
func DefaultLimits() Limits {
	return Limits{
		MaxIDBytes:             128,
		MaxDestinationBytes:    256,
		MaxMessageTypeBytes:    256,
		MaxAggregateTypeBytes:  256,
		MaxAggregateIDBytes:    512,
		MaxOrderingKeyBytes:    512,
		MaxIdempotencyKeyBytes: 512,
		MaxPayloadBytes:        1024 * 1024,
		MaxHeaders:             64,
		MaxHeaderKeyBytes:      128,
		MaxHeaderValueBytes:    4096,
		MaxHeaderBytes:         16 * 1024,
		MaxErrorCodeBytes:      128,
		MaxErrorMessageBytes:   4096,
		MaxLeaseOwnerBytes:     256,
		MaxLeaseTokenBytes:     256,
		MaxBatchSize:           100,
		MaxClaimBatchSize:      100,
		MaxPageSize:            200,
		MaxQueryIDs:            200,
		MaxQueryValues:         200,
		MaxPurgeSize:           1000,
		MaxAttempts:            1000,
		MaxWorkerCount:         256,
		MaxDestinationWorkers:  256,
	}
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits()
	if l.MaxIDBytes <= 0 {
		l.MaxIDBytes = d.MaxIDBytes
	}
	if l.MaxDestinationBytes <= 0 {
		l.MaxDestinationBytes = d.MaxDestinationBytes
	}
	if l.MaxMessageTypeBytes <= 0 {
		l.MaxMessageTypeBytes = d.MaxMessageTypeBytes
	}
	if l.MaxAggregateTypeBytes <= 0 {
		l.MaxAggregateTypeBytes = d.MaxAggregateTypeBytes
	}
	if l.MaxAggregateIDBytes <= 0 {
		l.MaxAggregateIDBytes = d.MaxAggregateIDBytes
	}
	if l.MaxOrderingKeyBytes <= 0 {
		l.MaxOrderingKeyBytes = d.MaxOrderingKeyBytes
	}
	if l.MaxIdempotencyKeyBytes <= 0 {
		l.MaxIdempotencyKeyBytes = d.MaxIdempotencyKeyBytes
	}
	if l.MaxPayloadBytes <= 0 {
		l.MaxPayloadBytes = d.MaxPayloadBytes
	}
	if l.MaxHeaders <= 0 {
		l.MaxHeaders = d.MaxHeaders
	}
	if l.MaxHeaderKeyBytes <= 0 {
		l.MaxHeaderKeyBytes = d.MaxHeaderKeyBytes
	}
	if l.MaxHeaderValueBytes <= 0 {
		l.MaxHeaderValueBytes = d.MaxHeaderValueBytes
	}
	if l.MaxHeaderBytes <= 0 {
		l.MaxHeaderBytes = d.MaxHeaderBytes
	}
	if l.MaxErrorCodeBytes <= 0 {
		l.MaxErrorCodeBytes = d.MaxErrorCodeBytes
	}
	if l.MaxErrorMessageBytes <= 0 {
		l.MaxErrorMessageBytes = d.MaxErrorMessageBytes
	}
	if l.MaxLeaseOwnerBytes <= 0 {
		l.MaxLeaseOwnerBytes = d.MaxLeaseOwnerBytes
	}
	if l.MaxLeaseTokenBytes <= 0 {
		l.MaxLeaseTokenBytes = d.MaxLeaseTokenBytes
	}
	if l.MaxBatchSize <= 0 {
		l.MaxBatchSize = d.MaxBatchSize
	}
	if l.MaxClaimBatchSize <= 0 {
		l.MaxClaimBatchSize = d.MaxClaimBatchSize
	}
	if l.MaxPageSize <= 0 {
		l.MaxPageSize = d.MaxPageSize
	}
	if l.MaxQueryIDs <= 0 {
		l.MaxQueryIDs = d.MaxQueryIDs
	}
	if l.MaxQueryValues <= 0 {
		l.MaxQueryValues = d.MaxQueryValues
	}
	if l.MaxPurgeSize <= 0 {
		l.MaxPurgeSize = d.MaxPurgeSize
	}
	if l.MaxAttempts <= 0 {
		l.MaxAttempts = d.MaxAttempts
	}
	if l.MaxWorkerCount <= 0 {
		l.MaxWorkerCount = d.MaxWorkerCount
	}
	if l.MaxDestinationWorkers <= 0 {
		l.MaxDestinationWorkers = d.MaxDestinationWorkers
	}
	return l
}

// Normalized returns a copy with safe defaults applied to zero-valued fields.
func (l Limits) Normalized() Limits { return l.withDefaults() }

var identifierPattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:/-]*$`)

// ValidateID checks the portable record ID syntax.
func ValidateID(id ID, limits Limits) error {
	limits = limits.withDefaults()
	text := string(id)
	if text == "" {
		return invalid("id", "is required")
	}
	if len(text) > limits.MaxIDBytes {
		return invalid("id", fmt.Sprintf("exceeds %d bytes", limits.MaxIDBytes))
	}
	if !identifierPattern.MatchString(text) {
		return invalid("id", "must begin with an alphanumeric character and contain only alphanumerics, '.', '_', ':', '/', or '-'")
	}
	return nil
}

// ValidateLeaseOwner checks the bounded worker identity stored with a claim.
func ValidateLeaseOwner(owner string, limits Limits) error {
	limits = limits.withDefaults()
	return validateRequiredText("lease_owner", owner, limits.MaxLeaseOwnerBytes)
}

// ValidateLeaseToken checks an injected token generator's bounded output. The
// default generator additionally provides cryptographic unpredictability.
func ValidateLeaseToken(token string, limits Limits) error {
	limits = limits.withDefaults()
	return validateRequiredText("lease_token", token, limits.MaxLeaseTokenBytes)
}

// ValidateDestination checks a portable destination value.
func ValidateDestination(destination string, limits Limits) error {
	limits = limits.withDefaults()
	return validateRequiredText("destination", destination, limits.MaxDestinationBytes)
}

// ValidateTimestamp checks the portable UTC microsecond range. A zero value is
// invalid; APIs where zero means "use the default" must apply that default
// before calling this function.
func ValidateTimestamp(field string, value time.Time) error {
	value = CanonicalTime(value)
	if value.IsZero() {
		return invalid(field, "is required")
	}
	minimum := time.Unix(0, 0).UTC()
	maximum := time.UnixMicro(MaximumTimestampUnixMicro).UTC()
	if value.Before(minimum) || value.After(maximum) {
		return invalid(field, fmt.Sprintf("must be between %s and %s", minimum.Format(time.RFC3339Nano), maximum.Format(time.RFC3339Nano)))
	}
	return nil
}

// ValidateLeaseDuration checks a lease duration against the portable
// microsecond precision and an adapter's configured upper bound.
func ValidateLeaseDuration(field string, duration, maximum time.Duration) error {
	if duration < time.Microsecond {
		return invalid(field, "must be at least one microsecond")
	}
	if maximum < time.Microsecond || duration > maximum {
		return invalid(field, fmt.Sprintf("must not exceed %s", maximum))
	}
	return nil
}

// PrepareRecord validates, defaults, copies, and digests an append request.
// Adapters call it before making any storage mutation.
func PrepareRecord(input NewRecord, now time.Time, defaultMaxAttempts int, limits Limits) (Record, error) {
	limits = limits.withDefaults()
	input = input.Clone()
	now = CanonicalTime(now)
	if err := ValidateTimestamp("clock", now); err != nil {
		return Record{}, err
	}
	if err := ValidateID(input.ID, limits); err != nil {
		return Record{}, err
	}
	if err := validateRequiredText("destination", input.Destination, limits.MaxDestinationBytes); err != nil {
		return Record{}, err
	}
	if err := validateRequiredText("message_type", input.MessageType, limits.MaxMessageTypeBytes); err != nil {
		return Record{}, err
	}
	if err := validateOptionalText("aggregate_type", input.AggregateType, limits.MaxAggregateTypeBytes); err != nil {
		return Record{}, err
	}
	if err := validateOptionalText("aggregate_id", input.AggregateID, limits.MaxAggregateIDBytes); err != nil {
		return Record{}, err
	}
	if err := validateOptionalText("ordering_key", input.OrderingKey, limits.MaxOrderingKeyBytes); err != nil {
		return Record{}, err
	}
	if err := validateOptionalText("idempotency_key", input.IdempotencyKey, limits.MaxIdempotencyKeyBytes); err != nil {
		return Record{}, err
	}
	if len(input.Payload) > limits.MaxPayloadBytes {
		return Record{}, invalid("payload", fmt.Sprintf("exceeds %d bytes", limits.MaxPayloadBytes))
	}
	if len(input.Headers) > limits.MaxHeaders {
		return Record{}, invalid("headers", fmt.Sprintf("exceeds %d entries", limits.MaxHeaders))
	}
	headerBytes := 0
	for key, value := range input.Headers {
		if key == "" || strings.TrimSpace(key) != key || len(key) > limits.MaxHeaderKeyBytes || strings.IndexByte(key, 0) >= 0 || !utf8.ValidString(key) {
			return Record{}, invalid("headers", "contains an invalid key")
		}
		if err := validateOptionalText("headers."+key, value, limits.MaxHeaderValueBytes); err != nil {
			return Record{}, err
		}
		headerBytes += len(key) + len(value)
	}
	if headerBytes > limits.MaxHeaderBytes {
		return Record{}, invalid("headers", fmt.Sprintf("exceed %d total bytes", limits.MaxHeaderBytes))
	}
	if input.MaxAttempts == 0 {
		input.MaxAttempts = defaultMaxAttempts
	}
	if input.MaxAttempts <= 0 || input.MaxAttempts > limits.MaxAttempts {
		return Record{}, invalid("max_attempts", fmt.Sprintf("must be between 1 and %d", limits.MaxAttempts))
	}
	if input.AvailableAt.IsZero() {
		input.AvailableAt = now
	}
	input.AvailableAt = CanonicalTime(input.AvailableAt)
	if err := ValidateTimestamp("available_at", input.AvailableAt); err != nil {
		return Record{}, err
	}
	digest := ImmutableDigest(input)
	return Record{
		ID: input.ID, Destination: input.Destination, MessageType: input.MessageType,
		AggregateType: input.AggregateType, AggregateID: input.AggregateID,
		OrderingKey: input.OrderingKey, IdempotencyKey: input.IdempotencyKey,
		Headers: input.Headers, Payload: input.Payload, ContentDigest: digest,
		State: StatePending, MaxAttempts: input.MaxAttempts,
		AvailableAt: input.AvailableAt, CreatedAt: now, UpdatedAt: now, Version: 1,
	}, nil
}

func validateRequiredText(field, value string, maximum int) error {
	if value == "" || strings.TrimSpace(value) != value {
		return invalid(field, "is required and must not have surrounding whitespace")
	}
	return validateOptionalText(field, value, maximum)
}

func validateOptionalText(field, value string, maximum int) error {
	if len(value) > maximum {
		return invalid(field, fmt.Sprintf("exceeds %d bytes", maximum))
	}
	if strings.IndexByte(value, 0) >= 0 {
		return invalid(field, "contains a NUL byte")
	}
	if !utf8.ValidString(value) {
		return invalid(field, "contains invalid UTF-8")
	}
	return nil
}

// ImmutableDigest returns the canonical digest used for duplicate comparison.
func ImmutableDigest(record NewRecord) string {
	hash := sha256.New()
	writeDigestString := func(value string) {
		var size [8]byte
		binary.BigEndian.PutUint64(size[:], uint64(len(value)))
		_, _ = hash.Write(size[:])
		_, _ = hash.Write([]byte(value))
	}
	writeDigestString(record.Destination)
	writeDigestString(record.MessageType)
	writeDigestString(record.AggregateType)
	writeDigestString(record.AggregateID)
	writeDigestString(record.OrderingKey)
	writeDigestString(record.IdempotencyKey)
	keys := make([]string, 0, len(record.Headers))
	for key := range record.Headers {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	for _, key := range keys {
		writeDigestString(key)
		writeDigestString(record.Headers[key])
	}
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(record.Payload)))
	_, _ = hash.Write(size[:])
	_, _ = hash.Write(record.Payload)
	return hex.EncodeToString(hash.Sum(nil))
}

// IIDGenerator supplies IDs for append requests that omit one.
type IIDGenerator interface{ NewID() (ID, error) }

// IDGeneratorFunc adapts a function to IIDGenerator.
type IDGeneratorFunc func() (ID, error)

func (f IDGeneratorFunc) NewID() (ID, error) { return f() }

// ITokenGenerator supplies unguessable per-attempt lease tokens.
type ITokenGenerator interface{ NewToken() (string, error) }

// TokenGeneratorFunc adapts a function to ITokenGenerator.
type TokenGeneratorFunc func() (string, error)

func (f TokenGeneratorFunc) NewToken() (string, error) { return f() }

// CryptoIDGenerator returns a cryptographically random 128-bit ID generator.
func CryptoIDGenerator() IIDGenerator {
	return IDGeneratorFunc(func() (ID, error) {
		var value [16]byte
		if _, err := rand.Read(value[:]); err != nil {
			return "", fmt.Errorf("generate outbox ID: %w", err)
		}
		return ID(hex.EncodeToString(value[:])), nil
	})
}

// CryptoTokenGenerator returns a cryptographically random 256-bit token generator.
func CryptoTokenGenerator() ITokenGenerator {
	return TokenGeneratorFunc(func() (string, error) {
		var value [32]byte
		if _, err := rand.Read(value[:]); err != nil {
			return "", fmt.Errorf("generate outbox lease token: %w", err)
		}
		return base64.RawURLEncoding.EncodeToString(value[:]), nil
	})
}
