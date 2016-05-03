package topicbyevent

import "github.com/savaki/zephyr"

func StringValue(item map[string]zephyr.AttributeValue, key string) (string, bool) {
	if item == nil {
		return "", false
	}

	av, ok := item[key]
	if !ok {
		return "", false
	}

	if av.S == nil {
		return "", false
	}

	return *av.S, true
}
