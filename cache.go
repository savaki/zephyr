package zephyr

import "sync"

type cache struct {
	data map[string]*string
	mux  *sync.Mutex
}

func (c *cache) Get(topicName string) (*string, bool) {
	c.mux.Lock()
	defer c.mux.Unlock()

	arn, ok := c.data[topicName]
	return arn, ok
}

func (c *cache) Set(topicName string, topicArn *string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	c.data[topicName] = topicArn
}

func (c *cache) Delete(topicName string) {
	c.mux.Lock()
	defer c.mux.Unlock()

	delete(c.data, topicName)
}

func newCache() *cache {
	return &cache{
		data: map[string]*string{},
		mux:  &sync.Mutex{},
	}
}
