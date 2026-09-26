package store

import (
	"context"
	"time"

	"telemetry-pipeline/internal/detect"
)

// Cached keeps user state in memory in front of a durable Store. It is only
// correct for a single writer per user: the engine creates one per assigned
// partition (Kafka routes each user to one partition) and drops it when the
// partition is revoked, so a new owner starts by reading the durable copy.
//
// Reads hit the backing store only for users not yet cached; every write goes
// to the backing store first, then to the cache.
type Cached struct {
	backing  Store
	mem      *Memory
	lastUsed map[string]time.Time
	ttl      time.Duration
	loads    int
	now      func() time.Time
}

func NewCached(backing Store) *Cached {
	return &Cached{backing: backing, mem: NewMemory(), lastUsed: map[string]time.Time{}, ttl: stateTTL, now: time.Now}
}

func (c *Cached) Load(ctx context.Context, users []string) (map[string]*detect.UserState, error) {
	var missing []string
	for _, u := range users {
		if _, ok := c.lastUsed[u]; !ok {
			missing = append(missing, u)
		}
	}
	loaded, err := c.backing.Load(ctx, missing)
	if err != nil {
		return nil, err
	}
	now := c.now()
	for u, st := range loaded {
		c.mem.put(u, st)
	}
	for _, u := range users {
		c.lastUsed[u] = now
	}
	c.evictIdle(now)
	return c.mem.Load(ctx, users) // copies, so a failed batch leaves the cache untouched
}

func (c *Cached) Save(ctx context.Context, updates []Update) error {
	if err := c.backing.Save(ctx, updates); err != nil {
		return err
	}
	return c.mem.Save(ctx, updates)
}

// Len reports how many users are cached.
func (c *Cached) Len() int { return len(c.lastUsed) }

// evictIdle drops users idle longer than the durable state's TTL, checking
// every 1,000 loads.
func (c *Cached) evictIdle(now time.Time) {
	c.loads++
	if c.loads%1000 != 0 {
		return
	}
	var idle []string
	for u, t := range c.lastUsed {
		if now.Sub(t) > c.ttl {
			idle = append(idle, u)
			delete(c.lastUsed, u)
		}
	}
	c.mem.remove(idle)
}
