package zephyr

import "testing"

func TestCache(t *testing.T) {
	topicName := "hello"
	topicArn := "hello:arn"

	c := newCache()

	_, ok := c.Get(topicName)
	if ok {
		t.Errorf("expected false; got true")
	}

	// Set

	c.Set(topicName, &topicArn)

	v, ok := c.Get(topicName)
	if !ok {
		t.Errorf("expected true; got false")
	}
	if *v != topicArn {
		t.Errorf("expected %v; got %v", topicArn, *v)
	}

	// Delete

	c.Delete(topicName)

	_, ok = c.Get(topicName)
	if ok {
		t.Errorf("expected false; got true")
	}
}
