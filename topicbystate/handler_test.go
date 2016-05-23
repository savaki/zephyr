package topicbystate_test

import (
	"testing"

	"github.com/savaki/zephyr"
	"github.com/savaki/zephyr/topicbystate"
)

func TestCompiles(t *testing.T) {
}

func TestIdentifyEnv(t *testing.T) {
	r := zephyr.Record{
		EventSourceARN: "arn:aws:dynamodb:us-east-1:554068800329:table/rewards-tracy-orders/stream/2016-05-16T22:22:50.550",
	}
	env, ok := topicbystate.IdentifyEnv(r)
	if !ok {
		t.Error("expected #IdentifyEnv to be true")
		return
	}
	if env != "tracy" {
		t.Errorf("expected tracy; got %v", env)
	}
}
