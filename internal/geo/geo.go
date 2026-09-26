// Package geo holds geographic helpers.
package geo

import "math"

const earthRadiusMiles = 3958.8

// HaversineMiles returns the great-circle distance between two points.
func HaversineMiles(lat1, lon1, lat2, lon2 float64) float64 {
	rad := math.Pi / 180
	dLat := (lat2 - lat1) * rad
	dLon := (lon2 - lon1) * rad
	a := math.Sin(dLat/2)*math.Sin(dLat/2) +
		math.Cos(lat1*rad)*math.Cos(lat2*rad)*math.Sin(dLon/2)*math.Sin(dLon/2)
	return 2 * earthRadiusMiles * math.Asin(math.Min(1, math.Sqrt(a)))
}
