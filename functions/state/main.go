package main

import (
	"os"

	"github.com/apex/go-apex"
	"github.com/savaki/loggly"
	"github.com/savaki/zap"
	"github.com/savaki/zephyr"
	"github.com/savaki/zephyr/topicbystate"
)

func main() {
	var w zap.WriteSyncer = os.Stderr
	if token := os.Getenv("LOGGLY_TOKEN"); token != "" {
		client := loggly.New(token)
		w = zap.AddSync(client)
	}

	z := zephyr.New(
		zephyr.WithHandler(topicbystate.New("state")),
		zephyr.Output(w),
	)

	apex.HandleFunc(z)
}
