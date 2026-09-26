package detect

import "telemetry-pipeline/internal/event"

// Score thresholds (spec §4.5).
const (
	FlagScore  = 0.85
	BlockScore = 0.95
)

// Decide applies the decision table in spec §4.5. hasScore is false when the AI
// engine did not score the event (timeout, error, or too little history).
func Decide(velocity bool, score float64, hasScore bool) event.Action {
	switch {
	case hasScore && score >= BlockScore:
		return event.ActionBlockSession
	case velocity && hasScore && score >= FlagScore:
		return event.ActionBlockSession
	case velocity:
		return event.ActionFlagForReview
	case hasScore && score >= FlagScore:
		return event.ActionFlagForReview
	default:
		return event.ActionAllow
	}
}
