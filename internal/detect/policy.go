package detect

import "anti-scraping-pipeline/internal/event"

// Score thresholds and the sustained-evidence rule (spec §4.5).
const (
	FlagScore  = 0.85
	BlockScore = 0.95
	// A block needs SustainedHigh of the user's last SustainedOf scores at or
	// above FlagScore. One high score only flags: consecutive scores from a
	// human rarely stay high, and blocking on a single one blocked about 1 in
	// 13 simulated humans over 10 minutes.
	SustainedOf   = 10
	SustainedHigh = 8
)

// Decide applies the decision table in spec §4.5. hasScore is false when the AI
// engine did not score the event (timeout, error, or too little history).
// sustained reports whether the user's recent scores meet the block rule.
func Decide(velocity bool, score float64, hasScore, sustained bool) event.Action {
	switch {
	case hasScore && score >= BlockScore && sustained:
		return event.ActionBlockSession
	case velocity && hasScore && score >= FlagScore && sustained:
		return event.ActionBlockSession
	case velocity:
		return event.ActionFlagForReview
	case hasScore && score >= FlagScore:
		return event.ActionFlagForReview
	default:
		return event.ActionAllow
	}
}
