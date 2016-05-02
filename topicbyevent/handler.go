package topicbyevent

import (
	"bytes"
	"errors"
	"strings"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/zephyr"
)

var (
	ErrNilItem         = errors.New("zephyr:topicbyevent:err:item_nil")
	ErrEmptyKey        = errors.New("zephyr:topicbyevent:err:empty_key")
	ErrEmptyValue      = errors.New("zephyr:topicbyevent:err:empty_value")
	ErrInvalidEncoding = errors.New("zephyr:topicbyevent:err:invalid_encoding")
)

const (
	separator  = ","
	prefixJSON = "json" + separator
)

type Handler struct {
	Key string
}

func (h *Handler) TopicName(record zephyr.Record) (string, error) {
	switch record.EventName {
	case zephyr.Insert:
		return TopicName(record.Dynamodb.NewImage, h.Key)

	case zephyr.Modify:
		oldState, oldOk := StringValue(record.Dynamodb.OldImage, h.Key)
		newState, newOk := StringValue(record.Dynamodb.NewImage, h.Key)

		if newOk && (!oldOk || newState != oldState) {
			return TopicName(record.Dynamodb.NewImage, h.Key)
		}
	}

	return "", nil
}

func (h *Handler) ExtractMessage(record zephyr.Record) (string, error) {
	return Unmarshal(record.Dynamodb.NewImage, h.Key)
}

func New(key string) *Handler {
	return &Handler{
		Key: key,
	}
}

func TopicName(item map[string]*dynamodb.AttributeValue, key string) (string, error) {
	topicName := ""
	err := Parse(item, key, func(tn string, message string) error {
		topicName = tn
		return nil
	})
	return topicName, err
}

func Unmarshal(item map[string]*dynamodb.AttributeValue, key string) (string, error) {
	message := ""
	err := Parse(item, key, func(t string, m string) error {
		message = m
		return nil
	})
	return message, err
}

func Marshal(topic, key, value string) (*dynamodb.AttributeValue, error) {
	w := &bytes.Buffer{}
	w.WriteString(prefixJSON)
	w.WriteString(topic)
	w.WriteString(separator)
	w.WriteString(value)

	return &dynamodb.AttributeValue{
		S: aws.String(w.String()),
	}, nil
}

func Parse(item map[string]*dynamodb.AttributeValue, key string, fn func(string, string) error) error {
	if item == nil {
		return ErrNilItem
	}

	av, ok := item[key]
	if !ok {
		return ErrEmptyKey
	}

	if av == nil || av.S == nil {
		return ErrEmptyValue
	}

	raw := *av.S

	if !strings.HasPrefix(raw, prefixJSON) {
		return ErrInvalidEncoding
	}

	raw = raw[len(prefixJSON):]

	index := strings.Index(raw, separator)
	if index == -1 {
		return ErrInvalidEncoding
	}

	topicName := raw[:index]
	body := raw[index+1:]
	return fn(topicName, body)
}
