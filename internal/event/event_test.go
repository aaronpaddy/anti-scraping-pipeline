package event

import (
	"encoding/json"
	"reflect"
	"testing"
)

func TestTelemetryRoundTrip(t *testing.T) {
	raw := `{
  "event_id": "8f3b2a9c-7e1b-4d5a-9f2c-3a4b5c6d7e8f",
  "viewer_user_id": "usr_bot_9984120",
  "target_profile_id": "usr_prof_123456",
  "timestamp": 1790342400000,
  "action_type": "PROFILE_VIEW",
  "location_coordinates": {"latitude": 37.7749, "longitude": -122.4194},
  "device_signature": "mozilla/5.0_macos_apple_webkit"
}`
	var ev Telemetry
	if err := json.Unmarshal([]byte(raw), &ev); err != nil {
		t.Fatal(err)
	}
	if ev.Timestamp != 1790342400000 || ev.Location.Longitude != -122.4194 {
		t.Fatalf("unexpected decode: %+v", ev)
	}
	assertSameJSON(t, raw, ev)
}

func TestAlertRoundTrip(t *testing.T) {
	raw := `{
  "event_id": "8f3b2a9c-7e1b-4d5a-9f2c-3a4b5c6d7e8f",
  "viewer_user_id": "usr_bot_9984120",
  "event_timestamp": 1790342400000,
  "evaluated_at": 1790342400011,
  "geographic_velocity_triggered": true,
  "ai_scraping_anomaly_score": 0.965,
  "ai_score_available": true,
  "action_taken": "BLOCK_SESSION",
  "pipeline_processing_duration_ms": 11
}`
	var a Alert
	if err := json.Unmarshal([]byte(raw), &a); err != nil {
		t.Fatal(err)
	}
	if a.AnomalyScore == nil || *a.AnomalyScore != 0.965 || a.ActionTaken != ActionBlockSession {
		t.Fatalf("unexpected decode: %+v", a)
	}
	assertSameJSON(t, raw, a)
}

func TestAlertNullScore(t *testing.T) {
	b, _ := json.Marshal(Alert{ActionTaken: ActionFlagForReview})
	var m map[string]any
	_ = json.Unmarshal(b, &m)
	if v, ok := m["ai_scraping_anomaly_score"]; !ok || v != nil {
		t.Fatalf("score should be present and null, got %v", m)
	}
}

func assertSameJSON(t *testing.T, want string, v any) {
	t.Helper()
	got, _ := json.Marshal(v)
	var a, b any
	_ = json.Unmarshal([]byte(want), &a)
	_ = json.Unmarshal(got, &b)
	if !reflect.DeepEqual(a, b) {
		t.Fatalf("round trip mismatch\nwant %s\ngot  %s", want, got)
	}
}
