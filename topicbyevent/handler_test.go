package topicbyevent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/zephyr"
	"github.com/savaki/zephyr/topicbyevent"
)

func TestJSON(t *testing.T) {
	item := map[string]*dynamodb.AttributeValue{}
	itemKey := "item-key"
	topicName := "topic-name"
	contents := "contents"

	// When
	err := topicbyevent.Marshal(item, topicName, itemKey, contents)
	if err != nil {
		t.Errorf("expected nil error; got %v", err)
	}

	// Test - TopicName
	actualName, err := topicbyevent.TopicName(item, itemKey)
	if err != nil {
		t.Errorf("expected nil error; got %v", err)
	}
	if actualName != topicName {
		t.Errorf("expected %v; got %v", topicName, actualName)
	}

	// Test - UnmarshalJSON
	message, err := topicbyevent.Unmarshal(item, itemKey)
	if err != nil {
		t.Errorf("expected nil error; got %v", err)
	}
	if message != contents {
		t.Errorf("expected %v; got %v", contents, message)
	}
}

func TestMarshalNil(t *testing.T) {
	err := topicbyevent.Marshal(nil, "blah", "blah", "blah")
	if err != topicbyevent.ErrNilItem {
		t.Fail()
	}

	_, err = topicbyevent.Unmarshal(nil, "blah")
	if err != topicbyevent.ErrNilItem {
		t.Fail()
	}
}

func TestHandlerInterfaces(t *testing.T) {
	h := topicbyevent.New("blah")

	if v := zephyr.TopicNamer(h); v == nil {
		t.Error("expected Handler to implement zephyr.TopicNamer")
	}
	if v := zephyr.MessageExtractor(h); v == nil {
		t.Error("expected Handler to implement zephyr.MessageExtractor")
	}
}
