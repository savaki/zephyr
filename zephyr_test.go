package zephyr_test

import (
	"encoding/json"
	"os"
	"sync/atomic"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/savaki/zephyr"
)

func TestCompiles(t *testing.T) {
	message := `
{
	"Records": [
		{
		}
	]
}`

	var topicNameCount int32
	var publishCount int32
	var lookupTopicArnCount int32

	// Given
	handler := zephyr.New(
		zephyr.WithTopicNameFunc(func(record zephyr.Record) (string, error) {
			atomic.AddInt32(&topicNameCount, 1)
			return "blah", nil
		}),
		zephyr.WithPublishFunc(func(topicArn *string, message string) error {
			atomic.AddInt32(&publishCount, 1)
			return nil
		}),
		zephyr.WithFindTopicArnFunc(func(topicName string) (*string, error) {
			atomic.AddInt32(&lookupTopicArnCount, 1)
			return aws.String("blah"), nil
		}),
		zephyr.Output(os.Stderr),
	)

	// When
	_, err := handler.Handle(json.RawMessage(message), nil)

	// Then
	if err != nil {
		t.Errorf("expected nil error; got %v", err)
	}
	if topicNameCount != 1 {
		t.Errorf("expected topicNameCount == 1; got %v", topicNameCount)
	}
	if publishCount != 1 {
		t.Errorf("expected publishCount == 1; got %v", publishCount)
	}
	if lookupTopicArnCount != 1 {
		t.Errorf("expected lookupTopicArnCount == 1; got %v", lookupTopicArnCount)
	}
}
