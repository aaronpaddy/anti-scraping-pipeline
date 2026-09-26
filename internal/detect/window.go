package detect

import (
	"math"
	"sort"
)

// Activity window limits (spec §4.3).
const (
	WindowMs      = 5 * 60 * 1000
	WindowMax     = 200
	MinFeatureLen = 5 // fewer events than this are not scored by the AI engine
	FeatureDims   = 5
)

// Activity is one entry in a user's recent-activity window. Velocity is the
// event's velocity-check result, kept so a redelivered event can reuse it.
type Activity struct {
	EventID  string
	Target   string
	TS       int64
	Velocity bool
}

// Window is a user's recent activity, sorted by timestamp.
type Window struct {
	Items []Activity
}

// Add inserts an activity in timestamp order and trims the window to the last
// WindowMs and at most WindowMax entries. An event already in the window (a
// redelivery) is ignored.
func (w *Window) Add(a Activity) {
	i := sort.Search(len(w.Items), func(i int) bool { return w.Items[i].TS > a.TS })
	for j := i - 1; j >= 0 && w.Items[j].TS == a.TS; j-- {
		if w.Items[j].EventID == a.EventID {
			return
		}
	}
	w.Items = append(w.Items, Activity{})
	copy(w.Items[i+1:], w.Items[i:])
	w.Items[i] = a
	w.trim()
}

// Find returns the window entry for an event, if it is still in the window.
func (w *Window) Find(eventID string, ts int64) (Activity, bool) {
	i := sort.Search(len(w.Items), func(i int) bool { return w.Items[i].TS > ts })
	for j := i - 1; j >= 0 && w.Items[j].TS == ts; j-- {
		if w.Items[j].EventID == eventID {
			return w.Items[j], true
		}
	}
	return Activity{}, false
}

func (w *Window) trim() {
	if len(w.Items) == 0 {
		return
	}
	cutoff := w.Items[len(w.Items)-1].TS - WindowMs
	start := sort.Search(len(w.Items), func(i int) bool { return w.Items[i].TS >= cutoff })
	if n := len(w.Items) - start; n > WindowMax {
		start = len(w.Items) - WindowMax
	}
	if start > 0 {
		w.Items = append(w.Items[:0], w.Items[start:]...)
	}
}

// Features encodes the window as a vector for the AI engine. Every dimension is
// scaled into [0, 1]:
//
//	0: request rate (events/min, log scale, saturates at 600/min)
//	1: mean gap between events (seconds, log scale, saturates at 300s)
//	2: gap regularity (coefficient of variation / 2; bots are near 0)
//	3: distinct target profiles / events
//	4: window size (log scale, saturates at WindowMax)
//
// ok is false when the window has fewer than MinFeatureLen events.
func (w *Window) Features() (f []float32, ok bool) {
	n := len(w.Items)
	if n < MinFeatureLen {
		return nil, false
	}
	gaps := make([]float64, n-1)
	var sum float64
	for i := 1; i < n; i++ {
		gaps[i-1] = float64(w.Items[i].TS-w.Items[i-1].TS) / 1000
		sum += gaps[i-1]
	}
	mean := sum / float64(len(gaps))
	var sq float64
	for _, g := range gaps {
		sq += (g - mean) * (g - mean)
	}
	std := math.Sqrt(sq / float64(len(gaps)))
	cv := 0.0
	if mean > 0 {
		cv = std / mean
	}
	rate := 600.0 // span of zero means everything arrived at once
	if sum > 0 {
		rate = float64(n-1) / sum * 60
	}
	distinct := make(map[string]struct{}, n)
	for _, a := range w.Items {
		distinct[a.Target] = struct{}{}
	}
	return []float32{
		float32(clamp01(math.Log1p(rate) / math.Log1p(600))),
		float32(clamp01(math.Log1p(mean) / math.Log1p(300))),
		float32(clamp01(cv / 2)),
		float32(float64(len(distinct)) / float64(n)),
		float32(clamp01(math.Log1p(float64(n)) / math.Log1p(WindowMax))),
	}, true
}

func clamp01(x float64) float64 {
	return math.Max(0, math.Min(1, x))
}
