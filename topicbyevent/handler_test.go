package topicbyevent_test

import (
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/savaki/zephyr"
	"github.com/savaki/zephyr/topicbyevent"
)

func TestJSON(t *testing.T) {
	itemKey := "item-key"
	topicName := "topic-name"
	contents := "contents"

	// When
	item := map[string]*dynamodb.AttributeValue{
		itemKey: topicbyevent.Marshal(topicName, contents),
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

func TestHandlerInterfaces(t *testing.T) {
	h := topicbyevent.New("blah")

	if v := zephyr.TopicNamer(h); v == nil {
		t.Error("expected Handler to implement zephyr.TopicNamer")
	}
	if v := zephyr.MessageExtractor(h); v == nil {
		t.Error("expected Handler to implement zephyr.MessageExtractor")
	}
}
