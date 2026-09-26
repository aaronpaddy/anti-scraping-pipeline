package gen

import (
	"encoding/json"
	"os"
)

// Truth records which persona each simulated user is, so detection accuracy
// can be measured afterwards. Personas never appear in the event payload.
type Truth struct {
	Seed      uint64             `json:"seed"`
	StartedAt int64              `json:"started_at_ms"`
	Users     map[string]Persona `json:"users"`
}

func WriteTruth(path string, t Truth) error {
	b, err := json.Marshal(t)
	if err != nil {
		return err
	}
	return os.WriteFile(path, b, 0o644)
}

func ReadTruth(path string) (Truth, error) {
	var t Truth
	b, err := os.ReadFile(path)
	if err != nil {
		return t, err
	}
	return t, json.Unmarshal(b, &t)
}
