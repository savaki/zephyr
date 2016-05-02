package topicbystate

import (
	"errors"
	"fmt"
	"strings"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/zephyr"
)

var (
	ErrInvalidARN      = errors.New("Invalid arn format")
	ErrStateNotFound   = errors.New("Item has no state attribute")
	ErrStateNotString  = errors.New("State attribute not of string type")
	ErrStateNotChanged = errors.New("State record was not updated")
)

func New(state string) zephyr.TopicNamer {
	var fn zephyr.TopicNameFunc = func(record zephyr.Record) (string, error) {
		if record.EventName != zephyr.Insert && record.EventName != zephyr.Modify {
			return "", nil
		}

		segments := strings.Split(record.EventSourceARN, "/")
		if len(segments) < 2 {
			return "", ErrInvalidARN
		}

		fqTableName := segments[1]

		newState, err := State(state, record.Dynamodb.NewImage)
		if err != nil {
			return "", err
		}

		topicName := fmt.Sprintf("%v-%v", fqTableName, newState)

		if record.EventName == zephyr.Insert {
			return topicName, nil
		}

		oldState, err := State(state, record.Dynamodb.OldImage)
		if err != nil {
			return "", err
		}

		if newState == oldState {
			return "", ErrStateNotChanged
		}

		return topicName, nil
	}

	return fn
}

func State(state string, item map[string]*dynamodb.AttributeValue) (string, error) {
	value, ok := item[state]
	if !ok {
		return "", ErrStateNotFound
	}

	if value.S == nil {
		return "", ErrStateNotString
	}

	return *value.S, nil
}
