// Package event defines the payloads exchanged between pipeline services (spec §3).
package event

// Topic names (spec §4.2).
const (
	ClickstreamTopic = "platform-telemetry-clickstream"
	AlertsTopic      = "telemetry-anomaly-alerts"
)

// Coordinates is a WGS84 latitude/longitude pair.
type Coordinates struct {
	Latitude  float64 `json:"latitude"`
	Longitude float64 `json:"longitude"`
}

// Telemetry is one raw clickstream event (spec §3.1).
type Telemetry struct {
	EventID         string      `json:"event_id"`
	ViewerUserID    string      `json:"viewer_user_id"`
	TargetProfileID string      `json:"target_profile_id"`
	Timestamp       int64       `json:"timestamp"` // Unix epoch milliseconds
	ActionType      string      `json:"action_type"`
	Location        Coordinates `json:"location_coordinates"`
	DeviceSignature string      `json:"device_signature"`
}

// Action is the pipeline's decision for one event (spec §4.5).
type Action string

const (
	ActionAllow         Action = "ALLOW"
	ActionFlagForReview Action = "FLAG_FOR_REVIEW"
	ActionBlockSession  Action = "BLOCK_SESSION"
)

// Alert is one evaluated anomaly decision (spec §3.2). AnomalyScore is nil
// when the AI engine did not return a score.
type Alert struct {
	EventID                     string   `json:"event_id"`
	ViewerUserID                string   `json:"viewer_user_id"`
	EventTimestamp              int64    `json:"event_timestamp"`
	EvaluatedAt                 int64    `json:"evaluated_at"`
	GeographicVelocityTriggered bool     `json:"geographic_velocity_triggered"`
	AnomalyScore                *float64 `json:"ai_scraping_anomaly_score"`
	AIScoreAvailable            bool     `json:"ai_score_available"`
	ActionTaken                 Action   `json:"action_taken"`
	ProcessingDurationMs        float64  `json:"pipeline_processing_duration_ms"`
}

// ScoreItem is one entry of an AI engine batch request (spec §3.3).
type ScoreItem struct {
	EventID      string    `json:"event_id"`
	ViewerUserID string    `json:"viewer_user_id"`
	Features     []float32 `json:"features"`
}

type ScoreRequest struct {
	Items []ScoreItem `json:"items"`
}

type ScoreResult struct {
	EventID      string  `json:"event_id"`
	AnomalyScore float64 `json:"anomaly_score"`
}

type ScoreResponse struct {
	Results []ScoreResult `json:"results"`
}
