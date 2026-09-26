// Package detect holds the pure detection logic shared by the engine and the
// seed exporter: the geographic velocity check, the activity window and its
// features, and the decision policy.
package detect

import "telemetry-pipeline/internal/geo"

// Velocity thresholds (spec §4.3).
const (
	MinJumpMiles = 5.0   // ignore GPS/IP jitter below this distance
	MaxSpeedMPH  = 600.0 // roughly commercial-jet speed
)

// Session is a user's last known location.
type Session struct {
	Lat, Lon float64
	TS       int64 // epoch ms
	Valid    bool
}

// Observe runs the velocity check for an event against the stored location and
// then updates the location. Events older than the stored one are checked but do
// not overwrite it.
func (s *Session) Observe(lat, lon float64, ts int64) (triggered bool) {
	if !s.Valid {
		*s = Session{Lat: lat, Lon: lon, TS: ts, Valid: true}
		return false
	}
	d := geo.HaversineMiles(s.Lat, s.Lon, lat, lon)
	dt := ts - s.TS
	if d > MinJumpMiles {
		if dt <= 0 {
			triggered = true
		} else {
			hours := float64(dt) / 3.6e6
			triggered = d/hours > MaxSpeedMPH
		}
	}
	if ts >= s.TS {
		s.Lat, s.Lon, s.TS = lat, lon, ts
	}
	return triggered
}
