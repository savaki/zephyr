package topicbystate_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/jacobsa/oglematchers"
	"github.com/savaki/zephyr/topicbystate"
)

type Person struct {
	Name     string
	Age      int
	Children []Person
	Father   *Person
	Friends  map[string]Person
}

func TestNewAttributeValue(t *testing.T) {
	person := Person{
		Name: "Joe Public",
		Age:  45,
		Friends: map[string]Person{
			"Close": Person{
				Name: "Eve",
				Age:  40,
			},
		},
		Children: []Person{
			{
				Name: "Bill",
				Age:  15,
			},
		},
	}

	item, err := dynamodbattribute.ConvertTo(person)
	if err != nil {
		t.Errorf("expected nil; got %v", err)
		return
	}

	data, err := json.Marshal(topicbystate.NewAttributeValue(item))
	if err != nil {
		t.Errorf("expected nil; got %v", err)
		return
	}

	var restored dynamodb.AttributeValue
	err = json.Unmarshal(data, &restored)
	if err != nil {
		t.Errorf("expected nil; got %v", err)
		return
	}

	var saved Person
	err = dynamodbattribute.ConvertFromMap(restored.M, &saved)
	if err != nil {
		t.Errorf("expected nil; got %#v", err)
		return
	}

	err = oglematchers.DeepEquals(saved).Matches(person)
	if err != nil {
		t.Errorf("expected nil; got %v", err)
		return
	}
}
