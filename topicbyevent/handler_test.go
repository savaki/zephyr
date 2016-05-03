package topicbyevent

import (
	"testing"

	"github.com/savaki/zephyr"
)

func TestHandlerInterfaces(t *testing.T) {
	h := New("blah")

	if v := zephyr.TopicNamer(h); v == nil {
		t.Error("expected Handler to implement zephyr.TopicNamer")
	}
	if v := zephyr.MessageExtractor(h); v == nil {
		t.Error("expected Handler to implement zephyr.MessageExtractor")
	}
}
