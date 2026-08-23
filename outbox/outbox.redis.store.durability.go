package outbox

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// RedisDurabilityMode controls startup behavior when persistence cannot meet policy.
type RedisDurabilityMode string

const (
	RedisDurabilityUnchecked  RedisDurabilityMode = "unchecked"
	RedisDurabilityWarn       RedisDurabilityMode = "warn"
	RedisDurabilityRequireAOF RedisDurabilityMode = "require_aof"
)

// ErrRedisUnsafeDurability marks a required persistence or eviction policy failure.
var ErrRedisUnsafeDurability = errors.New("outbox redis: unsafe durability configuration")

// RedisDurabilityReport is a copied snapshot of inspected Redis settings and status.
type RedisDurabilityReport struct {
	Checked        bool
	AOFEnabled     bool
	AOFFsync       string
	AOFLastWriteOK bool
	EvictionPolicy string
	Role           string
	Warnings       []string
}

// CheckDurability refreshes persistence, eviction, and replication observations.
func (store *RedisStore) CheckDurability(ctx context.Context) (report RedisDurabilityReport, resultErr error) {
	if err := store.ensure(ctx); err != nil {
		return report, err
	}
	defer func() {
		copy := report
		copy.Warnings = append([]string(nil), report.Warnings...)
		store.durability.Store(&copy)
	}()
	if store.config.DurabilityMode == RedisDurabilityUnchecked && !store.config.RequireNoEviction {
		return RedisDurabilityReport{Checked: false}, nil
	}
	report = RedisDurabilityReport{Checked: true, AOFLastWriteOK: true}
	values := make(map[string]string, 3)
	for _, setting := range []string{"appendonly", "appendfsync", "maxmemory-policy"} {
		result, err := store.client.ConfigGet(ctx, setting).Result()
		if err != nil {
			if store.config.DurabilityMode == RedisDurabilityRequireAOF || store.config.RequireNoEviction {
				return report, fmt.Errorf("%w: CONFIG GET: %v", ErrRedisUnsafeDurability, err)
			}
			report.Warnings = append(report.Warnings, "Redis persistence and eviction configuration could not be inspected")
			return report, nil
		}
		for key, value := range result {
			values[key] = value
		}
	}
	report.AOFEnabled = strings.EqualFold(values["appendonly"], "yes")
	report.AOFFsync = strings.ToLower(values["appendfsync"])
	report.EvictionPolicy = strings.ToLower(values["maxmemory-policy"])
	if persistence, infoErr := store.client.Info(ctx, "persistence").Result(); infoErr == nil {
		info := parseInfo(persistence)
		if status := strings.ToLower(info["aof_last_write_status"]); status != "" && status != "ok" {
			report.AOFLastWriteOK = false
			report.Warnings = append(report.Warnings, "Redis reports an AOF write failure")
		}
	} else {
		report.Warnings = append(report.Warnings, "Redis persistence INFO could not be inspected")
		if store.config.DurabilityMode == RedisDurabilityRequireAOF {
			return report, fmt.Errorf("%w: persistence INFO is required: %v", ErrRedisUnsafeDurability, infoErr)
		}
	}
	if replication, infoErr := store.client.Info(ctx, "replication").Result(); infoErr == nil {
		report.Role = parseInfo(replication)["role"]
	} else {
		report.Warnings = append(report.Warnings, "Redis replication INFO could not be inspected")
	}
	if !report.AOFEnabled {
		report.Warnings = append(report.Warnings, "Redis AOF persistence is disabled")
	}
	if report.AOFEnabled && report.AOFFsync == "everysec" {
		report.Warnings = append(report.Warnings, "Redis AOF everysec can lose a small recent window")
	}
	if report.EvictionPolicy != "noeviction" {
		report.Warnings = append(report.Warnings, "Redis maxmemory-policy is not noeviction")
	}
	if store.config.DurabilityMode == RedisDurabilityRequireAOF && (!report.AOFEnabled || !report.AOFLastWriteOK) {
		return report, fmt.Errorf("%w: healthy AOF is required", ErrRedisUnsafeDurability)
	}
	if store.config.RequireNoEviction && report.EvictionPolicy != "noeviction" {
		return report, fmt.Errorf("%w: noeviction is required", ErrRedisUnsafeDurability)
	}
	return report, nil
}

func parseInfo(value string) map[string]string {
	result := make(map[string]string)
	for _, line := range strings.Split(value, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		key, current, found := strings.Cut(line, ":")
		if found {
			result[key] = current
		}
	}
	return result
}
