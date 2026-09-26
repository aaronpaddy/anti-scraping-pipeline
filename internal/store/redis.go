package store

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/redis/go-redis/v9"

	"telemetry-pipeline/internal/detect"
)

const stateTTL = 30 * time.Minute

func sessionKey(user string) string  { return "session:v1:" + user }
func activityKey(user string) string { return "activity:v1:" + user }

// Activity members are "event_id|target_profile_id|velocity" (velocity is 0
// or 1), scored by timestamp.
func member(a detect.Activity) string {
	v := "0"
	if a.Velocity {
		v = "1"
	}
	return a.EventID + "|" + a.Target + "|" + v
}

func parseMember(m string, ts int64) detect.Activity {
	parts := strings.SplitN(m, "|", 3)
	a := detect.Activity{EventID: parts[0], TS: ts}
	if len(parts) > 1 {
		a.Target = parts[1]
	}
	if len(parts) > 2 {
		a.Velocity = parts[2] == "1"
	}
	return a
}

type Redis struct {
	c redis.UniversalClient
}

func NewRedis(c redis.UniversalClient) *Redis { return &Redis{c: c} }

func (r *Redis) Load(ctx context.Context, users []string) (map[string]*detect.UserState, error) {
	states := make(map[string]*detect.UserState, len(users))
	if len(users) == 0 {
		return states, nil
	}
	pipe := r.c.Pipeline()
	sessions := make([]*redis.MapStringStringCmd, len(users))
	windows := make([]*redis.ZSliceCmd, len(users))
	for i, u := range users {
		sessions[i] = pipe.HGetAll(ctx, sessionKey(u))
		windows[i] = pipe.ZRangeWithScores(ctx, activityKey(u), 0, -1)
	}
	if _, err := pipe.Exec(ctx); err != nil && err != redis.Nil {
		return nil, fmt.Errorf("redis load: %w", err)
	}
	for i, u := range users {
		st := &detect.UserState{}
		if h := sessions[i].Val(); len(h) > 0 {
			lat, e1 := strconv.ParseFloat(h["last_latitude"], 64)
			lon, e2 := strconv.ParseFloat(h["last_longitude"], 64)
			ts, e3 := strconv.ParseInt(h["last_timestamp"], 10, 64)
			if e1 == nil && e2 == nil && e3 == nil {
				st.Session = detect.Session{Lat: lat, Lon: lon, TS: ts, Valid: true}
			}
		}
		for _, z := range windows[i].Val() {
			m, _ := z.Member.(string)
			st.Window.Items = append(st.Window.Items, parseMember(m, int64(z.Score)))
		}
		states[u] = st
	}
	return states, nil
}

func (r *Redis) Save(ctx context.Context, updates []Update) error {
	if len(updates) == 0 {
		return nil
	}
	pipe := r.c.Pipeline()
	for _, up := range updates {
		if up.Session.Valid {
			sk := sessionKey(up.User)
			pipe.HSet(ctx, sk,
				"last_latitude", strconv.FormatFloat(up.Session.Lat, 'f', -1, 64),
				"last_longitude", strconv.FormatFloat(up.Session.Lon, 'f', -1, 64),
				"last_timestamp", strconv.FormatInt(up.Session.TS, 10))
			pipe.Expire(ctx, sk, stateTTL)
		}
		if len(up.Added) == 0 {
			continue
		}
		ak := activityKey(up.User)
		zs := make([]redis.Z, len(up.Added))
		var maxTS int64
		for i, a := range up.Added {
			zs[i] = redis.Z{Score: float64(a.TS), Member: member(a)}
			maxTS = max(maxTS, a.TS)
		}
		pipe.ZAdd(ctx, ak, zs...)
		// Trimming relative to this batch's newest event matches detect.Window
		// unless older state already holds a newer event; that only keeps a few
		// extra entries, which the next load trims in memory anyway.
		pipe.ZRemRangeByScore(ctx, ak, "-inf", "("+strconv.FormatInt(maxTS-detect.WindowMs, 10))
		pipe.ZRemRangeByRank(ctx, ak, 0, -detect.WindowMax-1)
		pipe.Expire(ctx, ak, stateTTL)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("redis save: %w", err)
	}
	return nil
}
