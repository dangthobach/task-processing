package job

import (
	"math"
	"math/rand/v2"
	"time"
)

// NextRetry computes a bounded delay for the just-failed attempt (attempt starts at 1).
func NextRetry(now time.Time, attempt int, p RetryPolicy, random func() float64) time.Time {
	if random == nil {
		random = rand.Float64
	}
	delay := float64(p.InitialDelayMS)
	if p.Strategy == "EXPONENTIAL" && attempt > 1 {
		delay *= math.Pow(p.Multiplier, float64(attempt-1))
	}
	if p.MaxDelayMS > 0 && delay > float64(p.MaxDelayMS) {
		delay = float64(p.MaxDelayMS)
	}
	if p.JitterPct > 0 {
		delay *= 1 + ((random()*2)-1)*(p.JitterPct/100)
	}
	if delay < 0 {
		delay = 0
	}
	return now.Add(time.Duration(delay) * time.Millisecond)
}
