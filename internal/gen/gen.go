// Package gen simulates clickstream traffic from humans and bots (spec §4.1).
// A Simulator is deterministic: the same Config always yields the same stream.
package gen

import (
	"container/heap"
	"fmt"
	"math"
	"math/rand/v2"

	"telemetry-pipeline/internal/event"
)

// Persona is a simulated user type.
type Persona string

const (
	Human      Persona = "human"
	Scraper    Persona = "scraper"
	Teleporter Persona = "teleporter"
	// StealthScraper harvests profiles while trying to look human: randomized
	// sleeps, occasional long pauses, some revisits. Its remaining tell is that
	// uniform random sleeps are more regular than human timing.
	StealthScraper Persona = "stealth_scraper"
	// PowerUser is a human who browses many profiles quickly (a recruiter,
	// say). It looks bot-like on rate and distinct profiles.
	PowerUser Persona = "power_user"
)

// Personas lists every persona.
var Personas = []Persona{Human, PowerUser, Scraper, StealthScraper, Teleporter}

// IsBot reports whether the persona counts as a bot for accuracy metrics.
func (p Persona) IsBot() bool { return p != Human && p != PowerUser }

type Config struct {
	Seed          uint64
	Users         int
	ScraperPct    float64
	TeleporterPct float64
	StealthPct    float64
	PowerUserPct  float64
	StartMs       int64 // simulated clock start, epoch ms
}

const (
	humanMeanGapMs   = 10_000 // exponential gaps, mean 10s
	humanMinGapMs    = 500
	scraperGapMs     = 1_000 // one request per second, ±10%
	teleportProb     = 0.25  // chance a teleporter's next event is in another city
	humanFavoriteMin = 3
	humanFavoriteMax = 15
	favoriteProb     = 0.7
	profileSpace     = 1_000_000
	jitterDeg        = 0.01 // about 0.7 miles

	stealthMinGapMs   = 3_000 // uniform random sleep 3–12s
	stealthMaxGapMs   = 12_000
	stealthPauseProb  = 0.02 // occasional pause of 20–45s
	stealthPauseMinMs = 20_000
	stealthPauseMaxMs = 45_000
	stealthRevisit    = 0.15 // share of views that revisit a recent profile

	powerMeanGapMs = 4_000 // exponential gaps, mean 4s
	powerRevisit   = 0.2   // revisit a recent profile
	powerFavorite  = 0.1   // view a favorite; the rest are new profiles

	recentProfiles = 10
)

var cities = [][2]float64{
	{37.7749, -122.4194}, {40.7128, -74.0060}, {41.8781, -87.6298}, {29.7604, -95.3698},
	{47.6062, -122.3321}, {25.7617, -80.1918}, {39.7392, -104.9903}, {42.3601, -71.0589},
	{33.7490, -84.3880}, {34.0522, -118.2437}, {51.5074, -0.1278}, {48.8566, 2.3522},
	{52.5200, 13.4050}, {35.6762, 139.6503}, {1.3521, 103.8198}, {-33.8688, 151.2093},
	{19.0760, 72.8777}, {-23.5505, -46.6333}, {43.6532, -79.3832}, {55.7558, 37.6173},
}

var devices = []string{
	"mozilla/5.0_macos_apple_webkit",
	"mozilla/5.0_windows_chrome",
	"mozilla/5.0_linux_firefox",
	"mozilla/5.0_iphone_safari",
	"mozilla/5.0_android_chrome",
}

type user struct {
	id        string
	persona   Persona
	city      int
	device    string
	favorites []int
	nextProf  int   // scrapers walk profiles sequentially
	recent    []int // last few profiles viewed (stealth scrapers, power users)
	nextTS    int64
}

// Simulator produces events in simulated-time order.
type Simulator struct {
	rng   *rand.Rand
	users []*user
	queue userQueue
}

func New(cfg Config) *Simulator {
	rng := rand.New(rand.NewPCG(cfg.Seed, cfg.Seed^0x9e3779b97f4a7c15))
	s := &Simulator{rng: rng}
	for i := 0; i < cfg.Users; i++ {
		u := &user{
			id:     fmt.Sprintf("usr_s%d_%07d", cfg.Seed, i),
			city:   rng.IntN(len(cities)),
			device: devices[rng.IntN(len(devices))],
		}
		switch r := rng.Float64(); {
		case r < cfg.ScraperPct:
			u.persona = Scraper
			u.nextProf = rng.IntN(profileSpace)
		case r < cfg.ScraperPct+cfg.TeleporterPct:
			u.persona = Teleporter
		case r < cfg.ScraperPct+cfg.TeleporterPct+cfg.StealthPct:
			u.persona = StealthScraper
		case r < cfg.ScraperPct+cfg.TeleporterPct+cfg.StealthPct+cfg.PowerUserPct:
			u.persona = PowerUser
		default:
			u.persona = Human
		}
		n := humanFavoriteMin + rng.IntN(humanFavoriteMax-humanFavoriteMin+1)
		for j := 0; j < n; j++ {
			u.favorites = append(u.favorites, rng.IntN(profileSpace))
		}
		// Spread first events over one mean gap so traffic starts smoothly.
		u.nextTS = cfg.StartMs + rng.Int64N(humanMeanGapMs)
		s.users = append(s.users, u)
	}
	s.queue = make(userQueue, len(s.users))
	copy(s.queue, s.users)
	heap.Init(&s.queue)
	return s
}

// Personas returns each user's persona, keyed by viewer_user_id.
func (s *Simulator) Personas() map[string]Persona {
	m := make(map[string]Persona, len(s.users))
	for _, u := range s.users {
		m[u.id] = u.persona
	}
	return m
}

// Next returns the next event in simulated time and the persona that sent it.
func (s *Simulator) Next() (event.Telemetry, Persona) {
	u := s.queue[0]
	ev := event.Telemetry{
		EventID:         s.uuid(),
		ViewerUserID:    u.id,
		Timestamp:       u.nextTS,
		ActionType:      "PROFILE_VIEW",
		DeviceSignature: u.device,
	}

	var profile int
	switch u.persona {
	case Scraper:
		profile = u.nextProf
		u.nextProf = (u.nextProf + 1) % profileSpace
		u.nextTS += int64(float64(scraperGapMs) * (0.9 + 0.2*s.rng.Float64()))
	case StealthScraper:
		if r := s.rng.Float64(); r < stealthRevisit && len(u.recent) > 0 {
			profile = u.recent[s.rng.IntN(len(u.recent))]
		} else {
			profile = s.rng.IntN(profileSpace)
		}
		gap := stealthMinGapMs + s.rng.Int64N(stealthMaxGapMs-stealthMinGapMs)
		if s.rng.Float64() < stealthPauseProb {
			gap = stealthPauseMinMs + s.rng.Int64N(stealthPauseMaxMs-stealthPauseMinMs)
		}
		u.nextTS += gap
		u.remember(profile)
	case PowerUser:
		switch r := s.rng.Float64(); {
		case r < powerRevisit && len(u.recent) > 0:
			profile = u.recent[s.rng.IntN(len(u.recent))]
		case r < powerRevisit+powerFavorite:
			profile = u.favorites[s.rng.IntN(len(u.favorites))]
		default:
			profile = s.rng.IntN(profileSpace)
		}
		u.nextTS += max(humanMinGapMs, int64(s.rng.ExpFloat64()*powerMeanGapMs))
		u.remember(profile)
	case Teleporter:
		if s.rng.Float64() < teleportProb {
			u.city = (u.city + 1 + s.rng.IntN(len(cities)-1)) % len(cities)
		}
		profile = s.humanProfile(u)
		u.nextTS += s.humanGap()
	default:
		profile = s.humanProfile(u)
		u.nextTS += s.humanGap()
	}
	ev.TargetProfileID = fmt.Sprintf("usr_prof_%06d", profile)
	c := cities[u.city]
	ev.Location = event.Coordinates{
		Latitude:  round6(c[0] + (s.rng.Float64()-0.5)*jitterDeg),
		Longitude: round6(c[1] + (s.rng.Float64()-0.5)*jitterDeg),
	}
	heap.Fix(&s.queue, 0)
	return ev, u.persona
}

func (u *user) remember(profile int) {
	if len(u.recent) == recentProfiles {
		copy(u.recent, u.recent[1:])
		u.recent = u.recent[:recentProfiles-1]
	}
	u.recent = append(u.recent, profile)
}

func (s *Simulator) humanProfile(u *user) int {
	if s.rng.Float64() < favoriteProb {
		return u.favorites[s.rng.IntN(len(u.favorites))]
	}
	return s.rng.IntN(profileSpace)
}

func (s *Simulator) humanGap() int64 {
	return max(humanMinGapMs, int64(s.rng.ExpFloat64()*humanMeanGapMs))
}

func (s *Simulator) uuid() string {
	a, b := s.rng.Uint64(), s.rng.Uint64()
	a = a&^(0xf<<12) | 0x4<<12 // version 4
	b = b&^(0x3<<62) | 0x2<<62 // RFC 4122 variant
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", a>>32, (a>>16)&0xffff, a&0xffff, b>>48, b&0xffffffffffff)
}

func round6(x float64) float64 { return math.Round(x*1e6) / 1e6 }

// userQueue is a min-heap on next event time, ties broken by user id order.
type userQueue []*user

func (q userQueue) Len() int { return len(q) }
func (q userQueue) Less(i, j int) bool {
	if q[i].nextTS != q[j].nextTS {
		return q[i].nextTS < q[j].nextTS
	}
	return q[i].id < q[j].id
}
func (q userQueue) Swap(i, j int) { q[i], q[j] = q[j], q[i] }
func (q *userQueue) Push(x any)   { *q = append(*q, x.(*user)) }
func (q *userQueue) Pop() any {
	old := *q
	u := old[len(old)-1]
	*q = old[:len(old)-1]
	return u
}
