package forgettable

import (
	"fmt"
	"time"

	"github.com/garyburd/redigo/redis"
)

// Table holds the connection pool to the redis server / cluster.
type Table struct {
	Pool *redis.Pool
}

// InitTable initializes our connection pool with redis and returns
// the pool structure plus all attached Catalog behaviors.
func InitTable(host string, port int, password string, db int) *Table {
	return &Table{Pool: initPool(host, port, password, db)}
}

func initPool(host string, port int, password string, db int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:      3,
		IdleTimeout:  240 * time.Second,
		TestOnBorrow: ping,
		Dial: func() (redis.Conn, error) {
			address := fmt.Sprintf("%s:%d", host, port)
			options := make([]redis.DialOption, 1, 2)
			options[0] = redis.DialDatabase(db)
			if password != "" {
				options = append(options, redis.DialPassword(password))
			}
			c, err := redis.Dial("tcp", address, options...)
			return c, err
		},
	}
}

func ping(c redis.Conn, t time.Time) error {
	_, err := c.Do("PING")
	return err
}

// Open gets a connection to the redis-server from the pool.
func (t *Table) Open() redis.Conn {
	return t.Pool.Get()
}

// Close the pool to the redis catalog
func (t *Table) Close() error {
	return t.Pool.Close()
}

// CreateDelta creates a Delta with the given name. date is the last decayed date of the delta which defaults to now() if nil.
func (t *Table) CreateDelta(name string, lifetime time.Duration, date time.Time, replay bool) (*Delta, error) {
	if lifetime.Seconds() <= 0 {
		return nil, fmt.Errorf("mean lifetime of an observation must be set to a positive number")
	}
	// modify our set values based on Delta parameters
	if replay {
		date = time.Now().Add(-lifetime)
	} else if date.IsZero() {
		date = time.Now()
	}
	// we set the last decayed date of the secondary set to older than
	// the primary, in order to support retrospective observations.
	now := time.Now()
	duration := now.Sub(date) * NormTimeMult
	secondary := now.Add(-duration)

	delta := Delta{Name: name, table: t}

	// create the two observable sets..
	delta.CreateSet(delta.GetPrimaryKey(), lifetime, date)
	delta.CreateSet(delta.GetSecondaryKey(), lifetime*NormTimeMult, secondary)
	return &delta, nil
}

// FetchDelta retrieves an existing Delta.
func (t *Table) FetchDelta(name string) *Delta {
	return &Delta{Name: name, table: t}
}

// Add adds the specified member with the specified score to the sorted set stored at key.
func (t *Table) Add(key string, score float64, member string) error {
	c := t.Open()
	defer c.Close()
	_, err := c.Do("ZADD", key, FormatFloat(score), member)
	return err
}

// Get returns the integer for the specified key.
func (t *Table) Get(key string) (interface{}, error) {
	c := t.Open()
	defer c.Close()
	return c.Do("GET", key)
}

// Set the entry into redis under key
func (t *Table) Set(key string, entry interface{}) error {
	c := t.Open()
	defer c.Close()
	_, err := c.Do("SET", key, entry)
	return err
}

// IncrBy increments the score of member in the sorted set stored at key by increment
func (t *Table) IncrBy(key string, increment float64, member string) error {
	c := t.Open()
	defer c.Close()
	_, err := c.Do("ZINCRBY", key, FormatFloat(increment), member)
	return err
}

// FloatScore returns the score of member in the sorted set at key.
func (t *Table) FloatScore(key string, member string) (float64, error) {
	c := t.Open()
	defer c.Close()
	return redis.Float64(c.Do("ZSCORE", key, member))
}

// IntScore returns the score of member in the sorted set at key.
func (t *Table) IntScore(key string, member string) (int64, error) {
	c := t.Open()
	defer c.Close()
	return redis.Int64(c.Do("ZSCORE", key, member))
}

// RevScoreRange returns the range of values & score from highest to lowest for the given start / stop values.
func (t *Table) RevScoreRange(key string, start int, stop int) (map[string]float64, error) {
	c := t.Open()
	defer c.Close()
	return Float64Map(c.Do("ZREVRANGE", key, FormatInt(start), FormatInt(stop), "WITHSCORES"))
}

// RemoveRangeByScore removes all elements in the sorted set stored at key with a score between min and max (inclusive).
func (t *Table) RemoveRangeByScore(key string, min interface{}, max interface{}) {
	c := t.Open()
	defer c.Close()
	c.Do("ZREMRANGEBYSCORE", key, min, max)
}
