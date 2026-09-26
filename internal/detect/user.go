package detect

import "telemetry-pipeline/internal/event"

// UserState is everything the engine keeps per viewer.
type UserState struct {
	Session Session
	Window  Window
}

// Result is the rule-based outcome for one event.
type Result struct {
	Velocity    bool
	Features    []float32 // nil when there is too little history
	Redelivered bool      // the event was already applied; nothing changed
	Added       Activity  // the window entry written (zero when Redelivered)
}

// Evaluate runs the velocity check and updates the activity window for one
// event, returning the features to score. Events must be passed in order.
//
// An event still in the window was applied before (a Kafka redelivery): its
// stored velocity result is reused and the state is left alone, because the
// stored location has since moved on and re-running the check would compare
// the wrong pair of points.
func (u *UserState) Evaluate(ev *event.Telemetry) Result {
	if a, ok := u.Window.Find(ev.EventID, ev.Timestamp); ok {
		f, _ := u.Window.Features()
		return Result{Velocity: a.Velocity, Features: f, Redelivered: true}
	}
	v := u.Session.Observe(ev.Location.Latitude, ev.Location.Longitude, ev.Timestamp)
	a := Activity{EventID: ev.EventID, Target: ev.TargetProfileID, TS: ev.Timestamp, Velocity: v}
	u.Window.Add(a)
	f, _ := u.Window.Features()
	return Result{Velocity: v, Features: f, Added: a}
}
