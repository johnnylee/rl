package ratelimitx

import (
	"errors"
	"sync"
	"time"
)

var ErrBackoff = errors.New("Backoff")

type LimiterConfig struct {
	BurstLimit   int64         // Number of requests to allow to burst.
	FillPeriod   time.Duration // Add one call per period.
	MaxWaitCount int64         // Max number of waiting requests. 0 disables.
}

type Limiter struct {
	lock sync.Mutex

	fillPeriod  time.Duration
	minWaitTime time.Duration
	maxWaitTime time.Duration

	waitTime    time.Duration
	lastRequest time.Time
}

func New(conf LimiterConfig) *Limiter {
	if conf.BurstLimit < 0 {
		panic(conf.BurstLimit)
	}
	if conf.FillPeriod <= 0 {
		panic(conf.FillPeriod)
	}
	if conf.MaxWaitCount < 0 {
		panic(conf.MaxWaitCount)
	}

	return &Limiter{
		fillPeriod:  conf.FillPeriod,
		waitTime:    -conf.FillPeriod * time.Duration(conf.BurstLimit),
		minWaitTime: -conf.FillPeriod * time.Duration(conf.BurstLimit),
		maxWaitTime: conf.FillPeriod * time.Duration(conf.MaxWaitCount-1),
		lastRequest: time.Now(),
	}
}

func (lim *Limiter) limit() (time.Duration, error) {
	lim.lock.Lock()
	defer lim.lock.Unlock()

	dt := time.Since(lim.lastRequest)
	waitTime := lim.waitTime - dt
	if waitTime < lim.minWaitTime {
		waitTime = lim.minWaitTime
	} else if waitTime >= lim.maxWaitTime {
		return 0, ErrBackoff
	}

	lim.waitTime = waitTime + lim.fillPeriod
	lim.lastRequest = lim.lastRequest.Add(dt)

	return lim.waitTime, nil
}

// Apply the limiter to the calling thread. The function may sleep for up to
// maxWaitTime before returning. If the timeout would need to be more than
// maxWaitTime to enforce the rate limit, ErrBackoff is returned.
func (lim *Limiter) Limit() error {
	dt, err := lim.limit()
	time.Sleep(dt) // Will return immediately for dt <= 0.
	return err
}
