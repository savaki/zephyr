package topicbystate

import "github.com/aws/aws-sdk-go/service/dynamodb"

type AttributeValue struct {
	B    []byte                    `type:"blob"    json:",omitempty"`
	BOOL *bool                     `type:"boolean" json:",omitempty"`
	BS   [][]byte                  `type:"list"    json:",omitempty"`
	L    []AttributeValue          `type:"list"    json:",omitempty"`
	M    map[string]AttributeValue `type:"map"     json:",omitempty"`
	N    *string                   `type:"string"  json:",omitempty"`
	NS   []*string                 `type:"list"    json:",omitempty"`
	NULL *bool                     `type:"boolean" json:",omitempty"`
	S    *string                   `type:"string"  json:",omitempty"`
	SS   []*string                 `type:"list"    json:",omitempty"`
}

func newList(in []*dynamodb.AttributeValue) []AttributeValue {
	if in == nil {
		return nil
	}

	out := make([]AttributeValue, len(in))
	for index, value := range in {
		out[index] = NewAttributeValue(value)
	}
	return out
}

func newMap(in map[string]*dynamodb.AttributeValue) map[string]AttributeValue {
	if in == nil {
		return nil
	}

	out := map[string]AttributeValue{}
	for key, value := range in {
		out[key] = NewAttributeValue(value)
	}
	return out
}

func NewAttributeValue(src *dynamodb.AttributeValue) AttributeValue {
	switch {
	case src.B != nil:
		return AttributeValue{B: src.B}
	case src.BOOL != nil:
		return AttributeValue{BOOL: src.BOOL}
	case src.BS != nil:
		return AttributeValue{BS: src.BS}
	case src.L != nil:
		return AttributeValue{L: newList(src.L)}
	case src.M != nil:
		return AttributeValue{M: newMap(src.M)}
	case src.N != nil:
		return AttributeValue{N: src.N}
	case src.NS != nil:
		return AttributeValue{NS: src.NS}
	case src.NULL != nil:
		return AttributeValue{NULL: src.NULL}
	case src.S != nil:
		return AttributeValue{S: src.S}
	case src.SS != nil:
		return AttributeValue{SS: src.SS}
	default:
		return AttributeValue{}
	}
}

type Record struct {
	Keys     map[string]AttributeValue
	NewImage map[string]AttributeValue
	OldImage map[string]AttributeValue
}
