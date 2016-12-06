package forgettable

import (
	"fmt"
	"time"
)

// NormTimeMult is the time multiplier for normalizing the Set
const NormTimeMult = 2

// Delta describes a trend by using two sets of counters.
type Delta struct {
	Name      string
	table     *Table
	primary   *Set
	secondary *Set
}

// CreateSet creates a sorted set in redis to hold our observables
func (d *Delta) CreateSet(name string, duration time.Duration, lastDecayDate time.Time) *Set {
	set := &Set{Name: name, table: d.table}
	set.UpdateDecayDate(lastDecayDate)
	set.InitLifetime(duration)
	return set
}

// Scores returns the trending scores for all entries
func (d *Delta) Scores() (map[string]float64, error) {
	counts, err := d.Primary().AllScores()
	if err != nil {
		return nil, err
	}
	norm, err := d.Secondary().AllScores()
	if err != nil {
		return nil, err
	}
	// make our map to hold normalized results
	result := make(map[string]float64, len(counts))
	for k, v := range counts {
		normV := norm[k]
		if normV == 0.0 {
			result[k] = 0.0
		} else {
			result[k] = v / normV
		}
	}
	return result, nil
}

// Incr increments the given bin by 1 for the current datetime.
func (d *Delta) Incr(bin string) {
	d.IncrBy(bin, 1.0, time.Now())
}

// IncrBy increments the given bin by amount for the given datetime.
func (d *Delta) IncrBy(bin string, amount float64, datetime time.Time) {
	if datetime.IsZero() {
		datetime = time.Now()
	}
	d.Primary().IncrBy(bin, amount, datetime)
	d.Secondary().IncrBy(bin, amount, datetime)
}

// Primary retrieves the primary set for this Delta
func (d *Delta) Primary() *Set {
	if d.primary == nil {
		d.primary = &Set{Name: d.GetPrimaryKey(), table: d.table}
	}
	return d.primary
}

// Secondary retrieves the secondary set for this Delta
func (d *Delta) Secondary() *Set {
	if d.secondary == nil {
		d.secondary = &Set{Name: d.GetSecondaryKey(), table: d.table}
	}
	return d.secondary
}

// GetPrimaryKey returns the primary key for the Delta
func (d *Delta) GetPrimaryKey() string {
	return d.Name
}

// GetSecondaryKey returns the secondary key
func (d *Delta) GetSecondaryKey() string {
	return fmt.Sprintf("%s_%dt", d.Name, NormTimeMult)
}
