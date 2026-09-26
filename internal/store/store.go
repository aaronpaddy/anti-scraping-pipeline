// Package store persists per-user detection state (spec §4.3 Redis keys).
package store

import (
	"context"

	"telemetry-pipeline/internal/detect"
)

// Update is the state to persist for one user after a batch.
type Update struct {
	User    string
	Session detect.Session
	Added   []detect.Activity // activities added in this batch
}

// Store loads and saves per-user state in one round trip each per batch.
type Store interface {
	// Load returns the current state of each user. Every requested user gets a
	// non-nil state.
	Load(ctx context.Context, users []string) (map[string]*detect.UserState, error)
	// Save persists user updates.
	Save(ctx context.Context, updates []Update) error
}
