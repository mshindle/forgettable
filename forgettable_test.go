package forgettable

import (
	"fmt"
	"testing"
	"time"
)

type category struct {
	name     string
	duration time.Duration
}

type observation struct {
	key      string
	datetime time.Time
}

var dfav = category{
	"favorites",
	168 * time.Hour,
}

var favorites = []observation{
	{"art_1", time.Now().AddDate(0, 0, -14)},
	{"art_2", time.Now().AddDate(0, 0, -10)},
	{"art_2", time.Now().AddDate(0, 0, -7)},
	{"art_1", time.Now().AddDate(0, 0, -1)},
	{"art_1", time.Now()},
}

func TestFavorites(t *testing.T) {
	table := InitTable("localhost", 6379, "", 0)
	defer table.Close()
	var initialDateTime time.Time
	delta, err := table.CreateDelta(dfav.name, dfav.duration, initialDateTime, true)
	if err != nil {
		t.Fatalf("Could not create delta: %v", err)
	}

	for _, o := range favorites {
		delta.IncrBy(o.key, 1.0, o.datetime)
	}

	scores, err := delta.Scores()
	if err != nil {
		t.Fatalf("could not pull scores: %v", err)
	}

	for k, v := range scores {
		fmt.Println("Key:", k, "Value:", v)
	}
}
