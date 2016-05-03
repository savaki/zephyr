package topicbystate

import (
	"errors"
	"fmt"
	"strings"

	"encoding/json"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/zephyr"
)

var (
	ErrInvalidARN      = errors.New("Invalid arn format")
	ErrStateNotFound   = errors.New("Item has no state attribute")
	ErrStateNotString  = errors.New("State attribute not of string type")
	ErrStateNotChanged = errors.New("State record was not updated")
)

type Handler struct {
	State string
}

func (h *Handler) TopicName(record zephyr.Record) (string, error) {
	if record.EventName != zephyr.Insert && record.EventName != zephyr.Modify {
		return "", nil
	}

	segments := strings.Split(record.EventSourceARN, "/")
	if len(segments) < 2 {
		return "", ErrInvalidARN
	}

	fqTableName := segments[1]

	newState, err := State(h.State, record.Dynamodb.NewImage)
	if err != nil {
		return "", err
	}

	topicName := fmt.Sprintf("%v-%v", fqTableName, newState)

	if record.EventName == zephyr.Insert {
		return topicName, nil
	}

	oldState, err := State(h.State, record.Dynamodb.OldImage)
	if err != nil {
		return "", err
	}

	if newState == oldState {
		return "", ErrStateNotChanged
	}

	return topicName, nil
}

func (h *Handler) ExtractMessage(record zephyr.Record) (string, error) {
	r := Record{
		Keys:     newMap(record.Dynamodb.Keys),
		NewImage: newMap(record.Dynamodb.NewImage),
		OldImage: newMap(record.Dynamodb.OldImage),
	}

	data, err := json.Marshal(r)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func New(state string) zephyr.TopicNamer {
	return &Handler{
		State: state,
	}
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
