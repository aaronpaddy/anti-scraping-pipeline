package detect

// ScoreHistory keeps each user's most recent AI scores to decide whether high
// scores are sustained. It is not safe for concurrent use: the engine keeps
// one per partition, used only by that partition's stage-2 goroutine, which
// sees each user's events in order.
type ScoreHistory struct {
	users   map[string]*recentScores
	records int
}

type recentScores struct {
	scores [SustainedOf]float64
	n      int // scores recorded, capped at SustainedOf
	next   int // ring position
	lastTS int64
}

// historyIdleMs drops users with no scored event for this long (event time).
const historyIdleMs = 30 * 60 * 1000

func NewScoreHistory() *ScoreHistory {
	return &ScoreHistory{users: map[string]*recentScores{}}
}

// Record adds a user's score for an event at ts and reports whether, including
// this one, SustainedHigh of the last SustainedOf scores are at least FlagScore.
func (h *ScoreHistory) Record(user string, ts int64, score float64) (sustained bool) {
	r := h.users[user]
	if r == nil {
		r = &recentScores{}
		h.users[user] = r
	}
	r.scores[r.next] = score
	r.next = (r.next + 1) % SustainedOf
	r.n = min(r.n+1, SustainedOf)
	r.lastTS = max(r.lastTS, ts)

	h.records++
	if h.records%100_000 == 0 {
		h.evictIdle(ts)
	}

	if r.n < SustainedOf {
		return false
	}
	high := 0
	for _, s := range r.scores {
		if s >= FlagScore {
			high++
		}
	}
	return high >= SustainedHigh
}

// Len returns the number of users tracked.
func (h *ScoreHistory) Len() int { return len(h.users) }

func (h *ScoreHistory) evictIdle(now int64) {
	for u, r := range h.users {
		if now-r.lastTS > historyIdleMs {
			delete(h.users, u)
		}
	}
}
