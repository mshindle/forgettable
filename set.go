package forgettable

import (
	"log"
	"math"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Set is a collection of observables stored in redis
// with an average observation lifetime and a start time.
type Set struct {
	Name  string
	table *Table
}

const lastDecaySuffix string = "_last_decay"
const lifetimeSuffix string = "_lifetime"
const scrubFilter float64 = 0.0001

// AllScores returns all scores stored in the set
func (s *Set) AllScores() (map[string]float64, error) {
	s.decay(time.Now())
	s.scrub()
	return s.fetch(-1)
}

// Incr increments the given bin by 1 for the current datetime.
func (s *Set) Incr(bin string) {
	s.IncrBy(bin, 1.0, time.Now())
}

// IncrBy increments the given bin by amount for the given datetime.
func (s *Set) IncrBy(bin string, amount float64, datetime time.Time) {
	if datetime.IsZero() {
		datetime = time.Now()
	}
	if s.validIncrDatetime(datetime) {
		s.table.IncrBy(s.Name, amount, bin)
	}
}

// InitLifetime creates the redis sorted set with the
// lifetime of the observation set to duration.
func (s *Set) InitLifetime(duration time.Duration) {
	s.table.Set(s.GetLifetimeKey(), duration)
}

// GetLifetime returns the duration of an observation for this set.
func (s *Set) GetLifetime() time.Duration {
	secs, _ := redis.Int64(s.table.Get(s.GetLifetimeKey()))
	return time.Duration(secs)
}

// GetLifetimeKey returns the lifetime value for this set
func (s *Set) GetLifetimeKey() string {
	return s.Name + lifetimeSuffix
}

// UpdateDecayDate sets the last decayed date to date
func (s *Set) UpdateDecayDate(datetime time.Time) error {
	return s.table.Set(s.GetLastDecayDateKey(), datetime.Unix())
}

// LastDecayDate returns the datetime of the last decay
func (s *Set) LastDecayDate() time.Time {
	secs, _ := redis.Int64(s.table.Get(s.GetLastDecayDateKey()))
	datetime := time.Unix(secs, 0)
	return datetime
}

// GetLastDecayDateKey returns the key which holds the LastDecayDate for this set.
func (s *Set) GetLastDecayDateKey() string {
	return s.Name + lastDecaySuffix
}

// Fetch retrieves scores from highest to lowest to either limit or all (-1).
func (s *Set) Fetch(limit int) (map[string]float64, error) {
	m, err := s.table.RevScoreRange(s.Name, 0, limit)
	if err != nil {
		return nil, err
	}
	return m, nil
}

func (s *Set) decay(datetime time.Time) {
	// open up a redis connection to pipeline against
	conn := s.table.Open()
	defer conn.Close()
	// get our parameters for decay
	deltaTime := datetime.Sub(s.LastDecayDate()).Seconds()
	lifetime := s.GetLifetime().Seconds()
	// loop through our entries
	conn.Send("MULTI")
	m, _ := s.Fetch(-1)
	for k, v := range m {
		decay := v * math.Exp(-deltaTime/lifetime)
		conn.Send("ZADD", s.Name, decay, k)
	}
	_, err := conn.Do("EXEC")
	if err != nil {
		log.Println("Could not decay set", s.Name, "=>", err)
		return
	}
	s.UpdateDecayDate(datetime)
}

func (s *Set) scrub() {
	s.table.RemoveRangeByScore(s.Name, "-inf", scrubFilter)
}

func (s *Set) validIncrDatetime(datetime time.Time) bool {
	lastDecayTime := s.LastDecayDate()
	return datetime.After(lastDecayTime)
}
