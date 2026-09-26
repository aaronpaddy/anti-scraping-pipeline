package store

import (
	"context"
	"fmt"
	"reflect"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"

	"anti-scraping-pipeline/internal/detect"
)

func stores(t *testing.T) map[string]Store {
	mr := miniredis.RunT(t)
	return map[string]Store{
		"redis":  NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()})),
		"memory": NewMemory(),
	}
}

func TestStoreRoundTrip(t *testing.T) {
	ctx := context.Background()
	for name, s := range stores(t) {
		t.Run(name, func(t *testing.T) {
			states, err := s.Load(ctx, []string{"u1"})
			if err != nil {
				t.Fatal(err)
			}
			if states["u1"] == nil || states["u1"].Session.Valid || len(states["u1"].Window.Items) != 0 {
				t.Fatalf("fresh load: %+v", states["u1"])
			}

			sess := detect.Session{Lat: 37.7749, Lon: -122.4194, TS: 5000, Valid: true}
			added := []detect.Activity{
				{EventID: "e1", Target: "p1", TS: 4000},
				{EventID: "e2", Target: "p2", TS: 5000, Velocity: true},
			}
			if err := s.Save(ctx, []Update{{User: "u1", Session: sess, Added: added}}); err != nil {
				t.Fatal(err)
			}
			states, err = s.Load(ctx, []string{"u1"})
			if err != nil {
				t.Fatal(err)
			}
			if states["u1"].Session != sess {
				t.Fatalf("session: %+v", states["u1"].Session)
			}
			if !reflect.DeepEqual(states["u1"].Window.Items, added) {
				t.Fatalf("window: %+v", states["u1"].Window.Items)
			}
		})
	}
}

func TestParseMemberCompat(t *testing.T) {
	// Members written before the velocity flag was added still load.
	if a := parseMember("e1|p1", 7); a != (detect.Activity{EventID: "e1", Target: "p1", TS: 7}) {
		t.Fatalf("old member: %+v", a)
	}
	if a := parseMember(member(detect.Activity{EventID: "e", Target: "p", Velocity: true}), 1); !a.Velocity || a.Target != "p" {
		t.Fatalf("round trip: %+v", a)
	}
}

func TestRedisTrimsWindow(t *testing.T) {
	ctx := context.Background()
	mr := miniredis.RunT(t)
	s := NewRedis(redis.NewClient(&redis.Options{Addr: mr.Addr()}))

	var added []detect.Activity
	for i := 0; i < detect.WindowMax+30; i++ {
		added = append(added, detect.Activity{EventID: fmt.Sprint("e", i), Target: "p", TS: int64(i) * 100})
	}
	old := detect.Activity{EventID: "old", Target: "p", TS: -detect.WindowMs}
	if err := s.Save(ctx, []Update{{User: "u", Added: append([]detect.Activity{old}, added...)}}); err != nil {
		t.Fatal(err)
	}
	states, _ := s.Load(ctx, []string{"u"})
	items := states["u"].Window.Items
	if len(items) != detect.WindowMax || items[0].EventID != "e30" {
		t.Fatalf("got %d items starting at %s", len(items), items[0].EventID)
	}
	if ttl := mr.TTL("activity:v1:u"); ttl != stateTTL {
		t.Fatalf("ttl = %v", ttl)
	}
	if keys := mr.Keys(); len(keys) != 1 {
		t.Fatalf("only the activity key should exist, got %v", keys)
	}
}
