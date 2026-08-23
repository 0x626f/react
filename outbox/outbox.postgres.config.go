package outbox

import (
	"fmt"
	"regexp"
	"time"
)

// PostgresConfig selects namespace, validated identifiers, and limits.
type PostgresConfig struct {
	Namespace          string
	Schema             string
	Table              string
	DuplicateMode      DuplicateMode
	DefaultMaxAttempts int
	MaxLeaseDuration   time.Duration
	Limits             Limits
}

// DefaultPostgresConfig returns adapter defaults for one shared table.
func DefaultPostgresConfig() PostgresConfig {
	return PostgresConfig{
		Namespace: "default", Schema: "react_outbox", Table: "records",
		DuplicateMode: RejectDuplicate, DefaultMaxAttempts: 10,
		MaxLeaseDuration: 5 * time.Minute, Limits: DefaultLimits(),
	}
}

var sqlIdentifier = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]{0,39}$`)
var postgresNamespacePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$`)

func (config PostgresConfig) normalized() (PostgresConfig, error) {
	defaults := DefaultPostgresConfig()
	if config.Namespace == "" {
		config.Namespace = defaults.Namespace
	}
	if !postgresNamespacePattern.MatchString(config.Namespace) {
		return PostgresConfig{}, fmt.Errorf("%w: invalid PostgreSQL namespace", ErrInvalidArgument)
	}
	if config.Schema == "" {
		config.Schema = defaults.Schema
	}
	if config.Table == "" {
		config.Table = defaults.Table
	}
	if !sqlIdentifier.MatchString(config.Schema) || !sqlIdentifier.MatchString(config.Table) {
		return PostgresConfig{}, fmt.Errorf("%w: invalid PostgreSQL schema or table identifier", ErrInvalidArgument)
	}
	if !config.DuplicateMode.Valid() {
		return PostgresConfig{}, fmt.Errorf("%w: duplicate mode", ErrInvalidArgument)
	}
	config.Limits = config.Limits.Normalized()
	if config.DefaultMaxAttempts == 0 {
		config.DefaultMaxAttempts = defaults.DefaultMaxAttempts
	}
	if config.DefaultMaxAttempts < 1 || config.DefaultMaxAttempts > config.Limits.MaxAttempts {
		return PostgresConfig{}, fmt.Errorf("%w: default max attempts", ErrInvalidArgument)
	}
	if config.MaxLeaseDuration == 0 {
		config.MaxLeaseDuration = defaults.MaxLeaseDuration
	}
	if config.MaxLeaseDuration < time.Microsecond {
		return PostgresConfig{}, fmt.Errorf("%w: max lease duration", ErrInvalidArgument)
	}
	return config, nil
}
