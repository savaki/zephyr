package topicbyevent

import "github.com/aws/aws-sdk-go/service/dynamodb"

func StringValue(item map[string]*dynamodb.AttributeValue, key string) (string, bool) {
	if item == nil {
		return "", false
	}

	av, ok := item[key]
	if !ok {
		return "", false
	}

	if av == nil || av.S == nil {
		return "", false
	}

	return *av.S, true
}
