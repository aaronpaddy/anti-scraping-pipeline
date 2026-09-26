package store

import (
	"context"
	"errors"
	"testing"
	"time"

	"telemetry-pipeline/internal/detect"
)

// countingStore wraps a Store and records which users were loaded from it.
type countingStore struct {
	Store
	loadedUsers []string
	failSave    bool
}

func (c *countingStore) Load(ctx context.Context, users []string) (map[string]*detect.UserState, error) {
	c.loadedUsers = append(c.loadedUsers, users...)
	return c.Store.Load(ctx, users)
}

func (c *countingStore) Save(ctx context.Context, u []Update) error {
	if c.failSave {
		return errors.New("down")
	}
	return c.Store.Save(ctx, u)
}

func TestCachedReadsBackingOncePerUser(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{Store: NewMemory()}
	sess := detect.Session{Lat: 1, Lon: 2, TS: 10, Valid: true}
	backing.Store.Save(ctx, []Update{{User: "u1", Session: sess}})

	c := NewCached(backing)
	states, _ := c.Load(ctx, []string{"u1"})
	if states["u1"].Session != sess {
		t.Fatalf("first load should come from backing: %+v", states["u1"])
	}
	next := detect.Session{Lat: 3, Lon: 4, TS: 20, Valid: true}
	if err := c.Save(ctx, []Update{{User: "u1", Session: next, Added: []detect.Activity{{EventID: "e", TS: 20}}}}); err != nil {
		t.Fatal(err)
	}
	states, _ = c.Load(ctx, []string{"u1"})
	if len(backing.loadedUsers) != 1 {
		t.Fatalf("backing loaded users %v, want only the first load", backing.loadedUsers)
	}
	if states["u1"].Session != next || len(states["u1"].Window.Items) != 1 {
		t.Fatalf("cached state: %+v", states["u1"])
	}
	// Backing got the write too.
	durable, _ := backing.Store.Load(ctx, []string{"u1"})
	if durable["u1"].Session != next {
		t.Fatal("write did not reach backing store")
	}
}

func TestCachedLoadReturnsCopies(t *testing.T) {
	ctx := context.Background()
	c := NewCached(NewMemory())
	states, _ := c.Load(ctx, []string{"u1"})
	states["u1"].Session = detect.Session{Lat: 9, Valid: true} // mutated by a batch that then fails
	again, _ := c.Load(ctx, []string{"u1"})
	if again["u1"].Session.Valid {
		t.Fatal("a failed batch's in-place changes leaked into the cache")
	}
}

func TestCachedFailedSaveLeavesCache(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{Store: NewMemory(), failSave: true}
	c := NewCached(backing)
	c.Load(ctx, []string{"u1"})
	if err := c.Save(ctx, []Update{{User: "u1", Session: detect.Session{Valid: true, TS: 5}}}); err == nil {
		t.Fatal("expected error")
	}
	states, _ := c.Load(ctx, []string{"u1"})
	if states["u1"].Session.Valid {
		t.Fatal("cache updated despite failed durable write")
	}
}

func TestCachedEvictsIdleUsers(t *testing.T) {
	ctx := context.Background()
	backing := &countingStore{Store: NewMemory()}
	c := NewCached(backing)
	now := time.Unix(0, 0)
	c.now = func() time.Time { return now }
	c.Load(ctx, []string{"idle"})
	now = now.Add(stateTTL + time.Minute)
	for i := 0; i < 1000; i++ {
		c.Load(ctx, []string{"active"})
	}
	if c.Len() != 1 {
		t.Fatalf("cached users = %d, want 1", c.Len())
	}
	backing.loadedUsers = nil
	c.Load(ctx, []string{"idle"})
	if len(backing.loadedUsers) != 1 {
		t.Fatal("evicted user should be reloaded from backing")
	}
}
