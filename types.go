package zephyr

import "github.com/savaki/zap"

// ---- EnvIdentifier -----------------------------------------------------------

type EnvIdentifierFunc func(record Record) (string, bool)

func (fn EnvIdentifierFunc) IdentifyEnv(record Record) (string, bool) {
	return fn(record)
}

type EnvIdentifier interface {
	IdentifyEnv(record Record) (string, bool)
}

// ---- TopicName ---------------------------------------------------------------

type TopicNameFunc func(record Record) (string, error)

func (fn TopicNameFunc) TopicName(record Record) (string, error) {
	return fn(record)
}

type TopicNamer interface {
	TopicName(record Record) (string, error)
}

// ---- TopicArnFinder ----------------------------------------------------------

type FindTopicArnFunc func(topicName string) (*string, error)

func (fn FindTopicArnFunc) FindTopicArn(topicName string) (*string, error) {
	return fn(topicName)
}

type TopicArnFinder interface {
	FindTopicArn(topicName string) (*string, error)
}

// ---- MessageExtractor --------------------------------------------------------

type ExtractMessageFunc func(record Record) (string, error)

func (fn ExtractMessageFunc) ExtractMessage(record Record) (string, error) {
	return fn(record)
}

type MessageExtractor interface {
	ExtractMessage(record Record) (string, error)
}

// ---- Publisher ---------------------------------------------------------------

type PublishFunc func(logger zap.Logger, topicArn *string, message string) error

func (fn PublishFunc) Publish(logger zap.Logger, topicArn *string, message string) error {
	return fn(logger, topicArn, message)
}

type Publisher interface {
	Publish(logger zap.Logger, topicArn *string, message string) error
}
