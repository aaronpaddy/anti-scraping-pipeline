package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"anti-scraping-pipeline/internal/event"
)

// Scorer returns an anomaly score per event id.
type Scorer interface {
	Score(ctx context.Context, items []event.ScoreItem) (map[string]float64, error)
}

// HTTPScorer calls the AI engine's POST /evaluate/batch (spec §3.3).
type HTTPScorer struct {
	url    string
	client *http.Client
}

func NewHTTPScorer(baseURL string) *HTTPScorer {
	return &HTTPScorer{
		url: baseURL + "/evaluate/batch",
		client: &http.Client{Transport: &http.Transport{
			MaxIdleConns:        64,
			MaxIdleConnsPerHost: 64,
			IdleConnTimeout:     90 * time.Second,
		}},
	}
}

func (s *HTTPScorer) Score(ctx context.Context, items []event.ScoreItem) (map[string]float64, error) {
	body, err := json.Marshal(event.ScoreRequest{Items: items})
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.url, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := s.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("ai engine: status %d", resp.StatusCode)
	}
	var out event.ScoreResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("ai engine: %w", err)
	}
	scores := make(map[string]float64, len(out.Results))
	for _, r := range out.Results {
		scores[r.EventID] = r.AnomalyScore
	}
	return scores, nil
}
