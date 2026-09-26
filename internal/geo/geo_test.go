package geo

import (
	"math"
	"testing"
)

func TestHaversineMiles(t *testing.T) {
	// San Francisco -> New York is about 2,570 miles.
	d := HaversineMiles(37.7749, -122.4194, 40.7128, -74.0060)
	if math.Abs(d-2570) > 15 {
		t.Fatalf("SF->NYC = %.1f miles", d)
	}
	if HaversineMiles(1, 2, 1, 2) != 0 {
		t.Fatal("same point should be 0")
	}
}
